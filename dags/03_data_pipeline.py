import os
import pandas as pd
from datetime import datetime
import sqlalchemy.exc

from airflow.models import DAG
from airflow import AirflowException

from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.etl import data_to_silver, data_to_gold
from utils.ml import anomaly
from utils.postgres_cli import PostgresClient
import utils.config_params as config


#PATH_GOLD = config.params["PATH_GOLD"]
sql_db = config.params["sql_db"]
sql_table = config.params["sql_table"]


def _search_anomaly(**context):
    year=str(context["logical_date"].year)
    df = anomaly(year)
    try:
        return df.to_json()
    except:
        raise AirflowException(f"There is no data for year {year}")
        #print(f"There is no data for year {year}")


# pylint: disable=no-member
def _data_to_database(**context):
    task_instance = context["ti"]
    df = pd.read_json(
        task_instance.xcom_pull(task_ids="search_anomaly"),
        orient="index",
    ).T
    df.drop(["index"], axis = 1, inplace=True)

    df['id'] = df['origin'] + df['fl_date'].str.replace(r'\D', '')
    cols=df.columns.tolist()
    cols=cols[-1:]+cols[:-1]
    df=df[cols]

    sql_cli = PostgresClient(sql_db)
    try:
        # for testing
        print(df)
        print(df.info())

        sql_cli.insert_from_frame(df, sql_table)
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        print("Data already exists! Nothing to do...")


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
    end_date=datetime(2019, 1, 1),
    schedule_interval='@yearly',
    default_args=default_args, 
    catchup=True) as dag:

    # task: 1
    begin = DummyOperator(task_id="begin")

    # task: 2
    data_to_silver = PythonOperator(
        task_id='data_to_silver',
        python_callable=data_to_silver
    )

    # task: 3
    data_to_gold = PythonOperator(
        task_id='data_to_gold',
        python_callable=data_to_gold
    )

    # task: 4
    create_aggregation_table = PostgresOperator(
        task_id="create_table_agg_dep_delay_by_date",
        postgres_conn_id='postgres',
        sql='sql/create_table_agg_dep_delay_by_date.sql'
    )

    # task: 5
    search_anomaly = PythonOperator(
        task_id='search_anomaly',
        python_callable=_search_anomaly
    )

    # task: 6
    data_to_database = PythonOperator(
        task_id="data_to_database", 
        python_callable=_data_to_database
    )

    # task: 7
    clean_data_folder = BashOperator(
        task_id="clean_data_folder",
        bash_command="""rm -f //opt/airflow/data/*""",
    )

    # task: 8
    end = DummyOperator(task_id="end")

    
    chain(
        begin,
        data_to_silver,
        data_to_gold,
        create_aggregation_table,
        search_anomaly,
        data_to_database,
        clean_data_folder,
        end,
    )