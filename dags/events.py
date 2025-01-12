import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow.utils
import airflow.utils.dates
import os
import polars as pl

def calculate_stats(input_path,output_path,**kwargs):
    execution_date = kwargs['ds']
    os.makedirs(f"{output_path}{execution_date}",exist_ok=True)
    data=pl.read_json(input_path)
    df_exploded = data.explode("user_events")
    df_final = df_exploded.unnest("user_events")
    writepath=f"{output_path}{execution_date}/stats.csv"
    df_final.write_csv(writepath)



dag=DAG(
    dag_id="events_api",
    schedule_interval='@daily',
    start_date=airflow.utils.dates.days_ago(1)
)

download_events=BashOperator(
    task_id="download_events",
    bash_command= "curl -o /opt/airflow/data/events.json  'http://event-generator1:5000/generate-events'",
    dag=dag
)

calculate_stats_op=PythonOperator(
    task_id="calculate_stats",
    python_callable=calculate_stats,
    op_kwargs={"input_path":"/opt/airflow/data/events.json","output_path":"/opt/airflow/data/"},
    provide_context=True,
    dag=dag
)


download_events >> calculate_stats_op