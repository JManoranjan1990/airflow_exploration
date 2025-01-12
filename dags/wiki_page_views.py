import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow.utils
import airflow.utils.dates
import requests
from urllib import request
import os




def download_pageviews(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    execution_date_str = execution_date.strftime('%Y-%m-%d')
    # https://dumps.wikimedia.org/other/pageviews/2024/2024-12/pageviews-20241201
    url= f'https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{int(hour)-4:0>2}0000.gz'
    output_path=f"/opt/airflow/data/{execution_date_str}/wiki_page_views.gz"
    print(url)
    print(output_path)
    os.makedirs(f"/opt/airflow/data/{execution_date_str}",exist_ok=True)
    request.urlretrieve(url, output_path)
    
    


dag=DAG(
    dag_id= "wikipageviews",
    schedule_interval="@daily",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    default_args={'owner':"Manoranjan"}
)



page_views=PythonOperator(
    task_id= "download_pageviews",
    python_callable=download_pageviews,
    op_args=[],
    # provide_context=True,
    dag=dag
)



extract_file=BashOperator(
    task_id="extract_file",
    bash_command="gunzip -c /opt/airflow/data/{{ ds }}/wiki_page_views.gz > /opt/airflow/data/{{ ds }}/wikipageviews.csv",
    dag=dag
)


page_views >> extract_file


# extract_file