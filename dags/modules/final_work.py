import pandas as pd
import dill
import datetime
from sqlalchemy import create_engine, select, Column, String, Integer, CHAR, Date, Time, schema, Table, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np
import os
import json
#path = os.path.expanduser('~/airflow_docker')
#os.environ['PROJECT_PATH'] = path
Base = declarative_base()
path_to_folder = '/opt/airflow/sources'


class Sessions(Base):
    __tablename__ = "sessions"
    session_id = Column("session_id", String, primary_key=True)
    client_id = Column("client_id", String)
    visit_date = Column("visit_date", Date)
    visit_time = Column("visit_time", Time)
    visit_number = Column('visit_number', Integer)
    utm_source = Column('utm_source', String)
    utm_medium = Column('utm_medium', String)
    utm_campaign = Column('utm_campaign', String)
    utm_adcontent = Column('utm_adcontent', String)
    utm_keyword = Column('utm_keyword', String)
    device_category = Column('device_category', String)
    device_os = Column('device_os', String)
    device_brand = Column('device_brand', String)
    device_model = Column('device_model', String)
    device_screen_resolution = Column('device_screen_resolution', String)
    device_browser = Column('device_browser', String)
    geo_country = Column('geo_country', String)
    geo_city = Column('geo_city', String)

    def __init__(self, session_id, client_id, visit_date, visit_time, visit_number, utm_source, utm_medium,
                 utm_adcontent, utm_campaign, utm_keyword, device_os, device_brand, device_model,
                 device_category, device_screen_resolution, device_browser, geo_country, geo_city):
        self.session_id = session_id
        self.client_id = client_id
        self.visit_date = visit_date
        self.visit_time = visit_time
        self.visit_number = visit_number
        self.utm_source = utm_source
        self.utm_medium = utm_medium
        self.utm_campaign = utm_campaign
        self.utm_adcontent = utm_adcontent
        self.utm_keyword = utm_keyword
        self.device_category = device_category
        self.device_os = device_os
        self.device_brand = device_brand
        self.device_model = device_model
        self.device_screen_resolution = device_screen_resolution
        self.device_browser = device_browser
        self.geo_country = geo_country
        self.geo_city = geo_city

    def __repr__(self):
        return f"{self.session_id} {self.client_id} {self.visit_date}"


class Hits(Base):
    __tablename__ = "hits"
    __table_args__ = {'extend_existing': True}
    hit_id = Column("hit_id", String, primary_key=True)
    session_id = Column(ForeignKey("sessions.session_id"))
    hit_date = Column("hit_date", Date)
    hit_time = Column("hit_time", Time)
    hit_number = Column("hit_number", Integer)
    hit_type = Column("hit_type", String)
    hit_referer = Column("hit_referer", String)
    hit_page_path = Column("hit_page_path", String)
    event_category = Column("event_category", String)
    event_action = Column("event_action", String)
    event_label = Column("event_label", String)
    event_value = Column("event_value", String)

    def __init__(self, hit_id, session_id, hit_date, hit_time, hit_number, hit_type, hit_referer, hit_page_path,
                 event_category, event_action, event_label, event_value):
        self.hit_id = hit_id
        self.session_id = session_id
        self.hit_date = hit_date
        self.hit_time = hit_time
        self.hit_number = hit_number
        self.hit_type = hit_type
        self.hit_referer = hit_referer
        self.hit_page_path = hit_page_path
        self.event_category = event_category
        self.event_action = event_action
        self.event_label = event_label
        self.event_value = event_value

    def __repr__(self):
        return f"{self.hit_id} {self.session_id} {self.hit_date}"


engine = create_engine("sqlite:///sources/sber_aito.db", echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)


def update_sessions(sessions_dataframe):
    sessions_dataframe['visit_date'] = sessions_dataframe['visit_date'].apply(pd.to_datetime)
    sessions_dataframe['visit_time'] = sessions_dataframe['visit_time'].apply(
        lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time())
    sessions_dataframe.drop_duplicates(subset=['session_id'])
    return sessions_dataframe


def update_hits(hits_dataframe):
    hits_dataframe['hit_date'] = hits_dataframe['hit_date'].apply(pd.to_datetime)
    hits_dataframe['hit_time'] = hits_dataframe['hit_time'].apply(
        lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time() if type(x) == 'str' else
        datetime.datetime.strptime("00:00:00", '%H:%M:%S').time())
    hits_dataframe['hit_id'] = (hits_dataframe['session_id'] +
                                hits_dataframe['hit_date'].astype('str') + hits_dataframe['hit_number'].astype('str'))
    hits_dataframe.drop_duplicates(subset=['hit_id'])
    print('hits updated')
    return hits_dataframe


def dataframe_to_database(dataframe, session, table_to_write_to):
    def existing_sessions(es_session):
        sql_query = """SELECT session_id 
        FROM sessions;
        """
        ses_ids = es_session.execute(sql_query)
        return [x[0] for x in ses_ids]

    def existing_hits(eh_session):
        sql_query = """SELECT hit_id 
        FROM hits;
        """
        ses_ids = eh_session.execute(sql_query)
        return [x[0] for x in ses_ids]
    log = []
    table = table_to_write_to
    if table == 'sessions':
        dataframe = update_sessions(dataframe)
        dataframe = dataframe[~dataframe['session_id'].isin(existing_sessions(session))]
#        list_of_obj = [Sessions(**kwargs) for kwargs in dataframe.to_dict(orient='records')]
    if table == 'hits':
        dataframe = update_hits(dataframe)
#        list_of_obj = [Hits(**kwargs) for kwargs in dataframe.to_dict(orient='records')]
        dataframe = dataframe[~dataframe['hit_id'].isin(existing_hits(session))]
    for chunk in np.array_split(dataframe, 10):
        chunk = chunk.drop_duplicates(subset=[chunk.columns[0]])
        chunk.to_sql(name=table, con=engine, if_exists='append', index=False)
        if table == 'sessions':
            added_ids = set(existing_sessions(session))
            assert set(chunk.loc[:, 'session_id']).issubset(added_ids)
        else:
            added_ids = set(existing_hits(session))
            assert set(chunk.loc[:, 'hit_id']).issubset(added_ids)


def upload_main_files():
    df_sessions_raw = pd.read_csv(f'{path_to_folder}/main_data/ga_sessions.csv')
    raw_df = pd.read_csv(f'{path_to_folder}/main_data/ga_hits.csv')
#    with open(f'{path_to_folder}/main_data/ga_hits-001.pkl', 'rb') as raw_file:
#        raw_df = dill.load(raw_file)
#        raw_file.flush()
#        print('after flush')
    with Session.begin() as session:
        print("Start Sessions")
        dataframe_to_database(df_sessions_raw, session, 'sessions')
        print("Start Hits")
        dataframe_to_database(raw_df, session, 'hits')
        session.commit()


def upload_new_files():
    new_sessions = pd.DataFrame()
    new_hits = pd.DataFrame()
    for json_file in os.listdir(f'{path_to_folder}/new_data'):
        with open(f'{path_to_folder}/new_data/{json_file}') as file:
            content = json.load(file)
            for i in content.values():
                df = pd.DataFrame(i)
            if 'ga_session' in json_file:
                if len(new_sessions):
                    new_sessions = pd.concat([new_sessions, df])
                else:
                    new_sessions = df
            else:
                if len(new_hits):
                    new_hits = pd.concat([new_hits, df])
                else:
                    new_hits = df
    with Session.begin() as session:
        print("Start Sessions")
        dataframe_to_database(new_sessions, session, 'sessions')
        print("Start Hits")
        dataframe_to_database(new_hits, session, 'hits')
        session.commit()


def main():
    #upload_main_files()
    upload_new_files()


if __name__ == '__main__':
    main()

