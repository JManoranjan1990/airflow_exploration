import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable
import os
import re

def failure_callback(context):
    """
    Function to handle failures and push failure details to XCom
    """
    ti= context['task_instance']
    error_message = str(context.get('exception'))
    task_id = ti.task_id
    dag_id = ti.dag_id
    ti.xcom_push(key='error_message', value=error_message)
    ti.xcom_push(key='failed_task_id', value=task_id)
    print(f"Task {task_id} failed with error: {error_message}")

    

def process_failure_info(**kwargs):
    """
    Task to pull the error message from XCom and process it (e.g., log it or take corrective actions)
    """
    ti = kwargs['ti']

    all_task_ids = kwargs['dag'].task_ids
    
    pattern = re.compile(r'^spark_submit_task.*')
    matching_task_ids = [task_id for task_id in all_task_ids if pattern.match(task_id)]

    for task_id in matching_task_ids:
        error_message = ti.xcom_pull(key='error_message', task_ids=task_id)
        if error_message:
            print(f"Error message for {task_id}: {error_message}")
        else:
            print(f"No error message for {task_id}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 13),
    'on_failure_callback': failure_callback,
}

dag = DAG(
    'spark_dynamic_dag',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
)


failure_task = PythonOperator(
    task_id='process_failure_info',
    python_callable=process_failure_info,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag,
)





def print_file():
    files_to_process=[]
    base_path = Variable.get("base_path")
    print(base_path)
    for file in os.listdir(base_path):
        print(base_path+file)

spark_tasks = []
base_path = Variable.get("base_path")
for file in os.listdir(base_path):
    task = SparkSubmitOperator(
        task_id=f'spark_submit_task_{file}',
        conn_id='spark-conn',  
        application='jobs/spark_insert.py', 
        name=f'spark_submit_task_{file}',
        application_args=[base_path+file,file],
        jars="/opt/bitnami/spark/data/postgresql-42.4.4.jar",
        conf={
            "spark.driver.extraClassPath": "/opt/bitnami/spark/data/postgresql-42.4.4.jar",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/data/postgresql-42.4.4.jar"
        },
        on_failure_callback=failure_callback,
        dag=dag, 
    )
    spark_tasks.append(task)

# print_filenames=PythonOperator(
#     task_id="print_filenames",
#     python_callable=print_file,
#     provide_context=True,
#     trigger_rule='one_failed',
#     dag=dag,
# )

spark_tasks >> failure_task
