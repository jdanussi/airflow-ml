import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args= {
    'owner': 'Jorge Danussi',
    'email': ['jdanussi@gmail.com'],
    'depends_on_past': False,
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id=DAG_ID,
    description='ETL pipeline',
    start_date=datetime(2008, 12, 31),
    end_date=datetime(2018, 1, 1),
    schedule_interval='@yearly',
    max_active_runs=3,
    default_args=default_args, 
    catchup=True) as dag:

    begin = DummyOperator(task_id="begin")

    trigger_dag_01 = TriggerDagRunOperator(
        task_id="trigger_dag_01",
        trigger_dag_id="01_etl_pipeline",
        wait_for_completion=True,
    )

    trigger_dag_02 = TriggerDagRunOperator(
        task_id="trigger_dag_02",
        trigger_dag_id="02_ml_pipeline",
        wait_for_completion=True,
    )

    end = DummyOperator(task_id="end")

chain(
    begin,
    trigger_dag_01,
    trigger_dag_02,
    end,
)
