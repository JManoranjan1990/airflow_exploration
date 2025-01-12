from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner':"Manoranjanj",
    'start_date':days_ago(1),
    'catchup': False
}

with DAG(dag_id="user_processing",
         default_args=default_args,
         schedule_interval='@daily') as dag:
    

    sensor=FileSensor(
        task_id="sensor",
        filepath="/opt/airflow/data/orders.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60*5
    )

    transformation_job=SparkSubmitOperator(
        task_id="pyspark_job",
        conn_id="spark-conn",
        application="jobs/pyspark_job.py",
        dag=dag)
    
    create_directory=BashOperator(
        task_id="create_directory",
        bash_command='mkdir -p /opt/airflow/data/_{{ ds }}',
        dag=dag,
    )

    move_file=BashOperator(
        task_id="movefile",
        bash_command= 'mv /opt/airflow/data/orders.csv /opt/airflow/data/_{{ ds }}',
        dag=dag
    )
    

    

    # sensor >> spark_job

    sensor>>transformation_job>>create_directory>>move_file
    

    

