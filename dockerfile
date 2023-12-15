FROM apache/airflow:2.7.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY reqs_final_work.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /reqs_final_work.txt
COPY --chown=airflow:root /dags /opt/airflow/dags
COPY --chown=airflow:root /sources/new_data /opt/airflow/new_data
COPY --chown=airflow:root /sources/main_data /opt/airflow/main_data
COPY --chown=airflow:root /modules /opt/airflow/modules
