"""etl_ml_pipeline module."""
# pylint: disable=invalid-name
# pylint: disable=import-error

import os
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy.exc

from airflow.models import DAG
from airflow.models import Variable
from airflow import AirflowException

from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook

from utils.etl import data_to_silver, data_to_gold
from utils.ml import anomaly
from utils.postgres_cli import PostgresClient

S3_BUCKET = Variable.get("data_lake_bucket")
S3_BRONZE = Variable.get("s3_bronze_folder")
PATH_LOCAL = Variable.get("local_path")
DB_URL = Variable.get("db_url")
DB_TABLE = Variable.get("db_table")


def _download_from_s3(key: str, bucket_name: str, local_path: str, **context) -> str:
    hook = S3Hook("s3_conn")
    logical_year = str(context["logical_date"].year)
    key = f"{S3_BRONZE}/{logical_year}.csv"

    file_name = hook.download_file(
        key=key, bucket_name=bucket_name, local_path=local_path
    )
    return file_name


def _rename_file(ti, **context) -> None:
    logical_year = str(context["logical_date"].year)
    new_file_name = str(f"{logical_year}_01_bronze.csv")

    downloaded_file_name = ti.xcom_pull(task_ids=["download_from_s3"])
    downloaded_file_path = "/".join(downloaded_file_name[0].split("/")[:-1])
    os.rename(
        src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_file_name}"
    )


def _search_anomaly(**context):
    logical_year = str(context["logical_date"].year)
    df = anomaly(logical_year)
    try:
        return df.to_json()
    except Exception as exc:
        raise AirflowException(f"There is no data for year {logical_year}") from exc


def _data_to_database(**context) -> None:
    task_instance = context["ti"]
    df = pd.read_json(
        task_instance.xcom_pull(task_ids="search_anomaly"),
        orient="index",
    ).T
    df.drop(["index"], axis=1, inplace=True)

    df["id"] = df["origin"] + df["fl_date"].str.replace(r"\D", "")
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    sql_cli = PostgresClient(DB_URL)
    try:
        # Write to log
        print(df)
        print(df.info())

        sql_cli.insert_from_frame(df, DB_TABLE)
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        print("Data already exists! Nothing to do...")


def _clean_data_folder(**context) -> None:
    logical_year = str(context["logical_date"].year)
    for file_name in os.listdir(PATH_LOCAL):
        if file_name.startswith(f"{logical_year}_"):
            os.remove(os.path.join(PATH_LOCAL, file_name))


DAG_ID = os.path.basename(__file__).replace(".py", "")

default_args = {
    "owner": "Jorge Danussi",
    "email": ["jdanussi@gmail.com"],
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="ETL pipeline",
    start_date=datetime(2008, 12, 31),
    end_date=datetime(2018, 1, 1),
    schedule_interval="@yearly",
    max_active_runs=3,
    default_args=default_args,
    catchup=True,
) as dag:

    # task: 1
    begin = DummyOperator(task_id="begin")

    # task: 2
    download_from_s3 = PythonOperator(
        task_id="download_from_s3",
        python_callable=_download_from_s3,
        op_kwargs={"key": "foo", "bucket_name": S3_BUCKET, "local_path": PATH_LOCAL},
    )

    # task: 3
    rename_file = PythonOperator(
        task_id="rename_file",
        python_callable=_rename_file,
    )

    # task: 4
    data_to_silver = PythonOperator(
        task_id="data_to_silver", python_callable=data_to_silver
    )

    # task: 5
    data_to_gold = PythonOperator(task_id="data_to_gold", python_callable=data_to_gold)

    # task: 6
    create_aggregation_table = PostgresOperator(
        task_id="create_table_agg_dep_delay_by_date",
        postgres_conn_id="postgres_conn",
        sql="sql/create_table_agg_dep_delay_by_date.sql",
    )

    # task: 7
    search_anomaly = PythonOperator(
        task_id="search_anomaly", python_callable=_search_anomaly
    )

    # task: 8
    data_to_database = PythonOperator(
        task_id="data_to_database", python_callable=_data_to_database
    )

    # task: 9
    clean_data_folder = PythonOperator(
        task_id="clean_data_folder", python_callable=_clean_data_folder
    )

    # task: 10
    end = DummyOperator(task_id="end")

    chain(
        begin,
        download_from_s3,
        rename_file,
        data_to_silver,
        data_to_gold,
        create_aggregation_table,
        search_anomaly,
        data_to_database,
        clean_data_folder,
        end,
    )
