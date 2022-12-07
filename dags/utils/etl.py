"""ETL module."""
# pylint: disable=invalid-name
# pylint: disable=import-error

import os
import pandas as pd

from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

# pylint: disable=invalid-name
PATH_LOCAL = Variable.get("local_path")
S3_BUCKET = Variable.get("data_lake_bucket")
S3_SILVER = Variable.get("s3_silver_folder")
S3_GOLD = Variable.get("s3_gold_folder")


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    """Upload files to S3."""
    hook = S3Hook("s3_conn")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)


def data_to_silver(**context):
    """Transform data to Silver."""
    logical_year = str(context["logical_date"].year)
    file_name_in = f"{logical_year}_01_bronze.csv"
    file_path_in = os.path.join(PATH_LOCAL, file_name_in)

    # checking if it is a file
    if os.path.isfile(file_path_in):

        df = pd.read_csv(file_path_in, sep=",")
        # df = df.iloc[0:100,:] # for testing

        # Change columns names to lower case
        df.columns = df.columns.str.lower()

        # Drop columns that start with 'unnamed'
        df = df[df.columns.drop(list(df.filter(regex="unnamed")))]

        # Drop rows with NaN in dep_delay column
        df = df[df["dep_delay"].notna()]

        file_name_out = f"{logical_year}_02_refined.parquet"
        file_path_out = os.path.join(PATH_LOCAL, file_name_out)
        df.to_parquet(file_path_out)

        # load result to S3
        key = f"{S3_SILVER}/{logical_year}/{file_name_out}"
        upload_to_s3(file_path_out, key, S3_BUCKET)

    else:
        print(f"File {file_path_in} don't exists")


def data_to_gold(**context):
    """Transform data to Gold."""

    logical_year = str(context["logical_date"].year)

    file_name_in = f"{logical_year}_02_refined.parquet"
    file_path_in = os.path.join(PATH_LOCAL, file_name_in)

    # checking if it is a file
    if os.path.isfile(file_path_in):

        df = pd.read_parquet(file_path_in)

        df_dep_avg_delay = (
            df.groupby(["origin", "fl_date"])["dep_delay"]
            .mean()
            .to_frame("mean_dep_delay")
            .reset_index()
        )

        df_dep_avg_delay["mean_dep_delay"] = df_dep_avg_delay["mean_dep_delay"].round(2)

        file_name_out = f"{logical_year}_03_agg.parquet"
        file_path_out = os.path.join(PATH_LOCAL, file_name_out)
        df_dep_avg_delay.to_parquet(file_path_out)

        # load result to S3
        key = f"{S3_GOLD}/{logical_year}/{file_name_out}"
        upload_to_s3(file_path_out, key, S3_BUCKET)

    else:
        print(f"File {file_path_in} don't exists")


if __name__ == "__main__":
    pass
