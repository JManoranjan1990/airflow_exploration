import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 13)
}

dag = DAG(
    dag_id='spark_read',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule or run it manually
    catchup=False,
)

# Define SparkSubmitOperator
spark_check = SparkSubmitOperator(
    task_id='spark_submit_job1',
    conn_id='spark-conn',  # Assuming you have the connection setup for Spark in Airflow
    application='jobs/pyspark_job.py',  # Path to your Spark script
    jars="/opt/bitnami/spark/data/postgresql-42.4.4.jar",
    conf={
        "spark.driver.extraClassPath": "/opt/bitnami/spark/data/postgresql-42.4.4.jar",
        "spark.executor.extraClassPath": "/opt/bitnami/spark/data/postgresql-42.4.4.jar"
    },
    dag=dag,
)

spark_check
