import airflow
from urllib import request
import json
import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow.utils
import airflow.utils.dates
from urllib import error
from airquality.aq import download_air_quality_index



dag=DAG(
    dag_id="Air_Quality_Index",
    schedule_interval="@hourly",
    catchup=False,
    description="extracts the data from the api.data.gov.in on a daily basis",
    start_date=airflow.utils.dates.days_ago(0),
    default_args={"owner":"ManoranjanJ"}
)





air_quality_index=PythonOperator(
    task_id="download_air_quality_index",
    python_callable=download_air_quality_index,
    op_kwargs={"output_path":"/opt/airflow/data/airquality/output/file"},
    dag=dag
)

air_quality_index