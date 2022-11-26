params = {
    "PATH_LOCAL": "/opt/airflow/data",
    "S3_BUCKET": "flights-datalake",
    "S3_BRONZE": "01_bronze",
    "S3_SILVER": "02_silver/year",
    "S3_GOLD": "03_gold/agg_dep_delay_by_date/year",
    "sql_db": "airflow:airflow@postgres/airflow",
    "sql_table": "agg_dep_delay_by_date",
    "db_schema": "public",
    "outliers_fraction": 0.01
}