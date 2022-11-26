import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from utils.etl import data_to_silver, data_to_gold


DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args= {
    'owner': 'Jorge Danussi',
    'email': ['jdanussi@gmail.com'],
    'depends_on_past': False,
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2008, 12, 31)
}

with DAG(
    dag_id=DAG_ID,
    description='ETL pipeline',
    schedule_interval='@yearly',
    #schedule_interval=None,
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1
    data_to_silver = PythonOperator(
        task_id='data_to_silver',
        python_callable=data_to_silver
    )

    # task: 2
    data_to_gold = PythonOperator(
        task_id='data_to_gold',
        python_callable=data_to_gold
    )

    data_to_silver >> data_to_gold
    