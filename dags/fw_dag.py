import datetime as dt
import os
import sys
sys.path.extend(["../modules"])
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from modules.final_work import upload_main_files
from modules.final_work import upload_new_files


# <YOUR_IMPORTS>

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}


with DAG(
        dag_id='db_upload',
        schedule="00 15 * * *",
        default_args=args,
) as dag:
    main_upload = PythonOperator(
        task_id='upload_main_files',
        python_callable=upload_main_files,
        priority_weight=3,
        weight_rule='downstream',
    )

    additional_upload = PythonOperator(
        task_id='upload_additional_files',
        python_callable=upload_new_files,
        priority_weight=1,
        weight_rule='downstream',
    )


main_upload >> additional_upload