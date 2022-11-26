import os
import pandas as pd
import sqlalchemy.exc

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from utils.postgres_cli import PostgresClient
import utils.config_params as config
from utils.ml import anomaly

PATH_GOLD = config.params["PATH_GOLD"]
sql_db = config.params["sql_db"]
sql_table = config.params["sql_table"]


def _search_anomaly(**context):
    #execution_date=context["logical_date"]
    #execution_year=execution_date.year
    year=str(context["logical_date"].year)
    df = anomaly(year)
    return df.to_json()


# pylint: disable=no-member
def _insert_daily_data(**context):
    task_instance = context["ti"]
    df = pd.read_json(
        task_instance.xcom_pull(task_ids="search_anomaly"),
        orient="index",
    ).T
    df = df.rename(columns={"index": "date"})
    sql_cli = PostgresClient(sql_db)
    try:
        sql_cli.insert_from_frame(df, sql_table)
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        print("Data already exists! Nothing to do...")
    
DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args= {
    'owner': 'Jorge Danussi',
    'email_on_failure': False,
    'email': ['jdanussi@gmail.com'],
    'start_date': datetime(2008, 12, 31)
}

with DAG(
    dag_id=DAG_ID,
    description='ML pipeline',
    schedule_interval='@yearly',
    #schedule_interval=None,
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1
    create_aggregation_table = PostgresOperator(
        task_id="create_table_agg_dep_delay_by_date",
        postgres_conn_id='postgres',
        sql='sql/create_table_agg_dep_delay_by_date.sql'
    )

    # task: 2
    search_anomaly = PythonOperator(
        task_id='search_anomaly',
        python_callable=_search_anomaly
    )

    # task: 3
    insert_daily_data = PythonOperator(
        task_id="insert_daily_data", 
        python_callable=_insert_daily_data
    )

    create_aggregation_table >> search_anomaly >> insert_daily_data