import os
import pandas as pd

from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
import utils.config_params as config

PATH_LOCAL = config.params["PATH_LOCAL"]
S3_BUCKET = config.params["S3_BUCKET"]
S3_BRONZE = config.params["S3_BRONZE"]
S3_SILVER = config.params["S3_SILVER"]
S3_GOLD = config.params["S3_GOLD"]


def s3_to_local(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('s3_conn')
    file = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)


def data_to_silver(**context):
    
    year=str(context["logical_date"].year)
    file_name = f"{year}.csv"

    key = f"{S3_BRONZE}/{file_name}"
    csv_file_in = s3_to_local(key, S3_BUCKET, PATH_LOCAL)    

    #file_name_out = f"{year}_02_refined.csv"
    file_name_out = f"{year}_02_refined.parquet"
    file_path_out = os.path.join(PATH_LOCAL, file_name_out) 
    
    df = pd.read_csv(csv_file_in, sep=',')
    #df = df.iloc[0:100,:] # for testing
    
    # Change columns names to lower case
    df.columns= df.columns.str.lower()

    # Drop columns that start with 'unnamed' 
    df = df[df.columns.drop(list(df.filter(regex='unnamed')))]

    # Drop rows with NaN in dep_delay column
    df = df[df['dep_delay'].notna()]

    #df.to_csv(file_path_out, sep=',', index=False)
    df.to_parquet(file_path_out)

    # load result to S3
    key = f"{S3_SILVER}/{year}/{file_name_out}"
    upload_to_s3(file_path_out, key, S3_BUCKET)


def data_to_gold(**context):    

    year=str(context["logical_date"].year)
    
    #file_name_in = f"{year}_02_refined.csv"
    file_name_in = f"{year}_02_refined.parquet"
    file_path_in = os.path.join(PATH_LOCAL, file_name_in)
    
    # checking if it is a file
    if os.path.isfile(file_path_in):
       
        #df = pd.read_csv(file_path_in, sep=',')
        df = pd.read_parquet(file_path_in)
        
        df_dep_avg_delay = df.groupby(["origin", "fl_date"])["dep_delay"].mean()\
            .to_frame('mean_dep_delay')\
            .reset_index()

        df_dep_avg_delay['mean_dep_delay'] = df_dep_avg_delay['mean_dep_delay']\
            .round(2)

        #file_name_out = f"{year}_03_agg.csv"
        file_name_out = f"{year}_03_agg.parquet"
        file_path_out = os.path.join(PATH_LOCAL, file_name_out)

        #df_dep_avg_delay.to_csv(file_path_out, sep=',', index=False)
        df_dep_avg_delay.to_parquet(file_path_out)

        # load result to S3
        key = f"{S3_GOLD}/{year}/{file_name_out}"
        upload_to_s3(file_path_out, key, S3_BUCKET)

    else:
        print(f"File {file_path_in} don't exists")


if __name__ == "__main__":
    pass