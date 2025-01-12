import airflow
import airflow.utils
import airflow.utils.dates
import requests
import os
import json
from airflow import DAG
from requests.exceptions import ConnectionError,MissingSchema
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def download_launch_images(**kwargs):
    execution_date = kwargs['ds']
    with open("/opt/airflow/data/rocket/lanuches.json") as f:
        launches=json.load(f)
    iamge_url=[launch['image'] for launch in launches['results']]
    os.makedirs(f"/opt/airflow/data/{execution_date}",exist_ok=True)
    for image in iamge_url:
        try:
            response=requests.get(image)
            file_name=image.split("/")[-1]
            target_file=f"/opt/airflow/data/{execution_date}/{file_name}"
            with open(target_file,"wb") as f:
                f.write(response.content)
        except MissingSchema:
            print(f"{image} seems to be a invlaid")
        except ConnectionError:
            print(f"{image} connection error")
        



dag=DAG(
    dag_id="download_launches",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(10)
)

download_launches=BashOperator(
    task_id="downlaod_rocket_lanches",
    bash_command= "curl -o /opt/airflow/data/rocket/lanuches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming/'",
    dag=dag
)

download_images=PythonOperator(
    task_id="download_launch_images",
    python_callable=download_launch_images,
    provide_context=True,
    dag=dag
)

notify=BashOperator(
    task_id="notify",
    bash_command='echo "there are $(/opt/airflow/data/{{ds}}/ | wc -l) Images" ',
    dag=dag
)

download_launches>>download_images >>notify