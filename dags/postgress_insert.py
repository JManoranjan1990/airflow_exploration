import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime



def failure_callback(context):
    """
    Function to handle failures and push failure details to XCom
    """
    ti= context['task_instance']
    error_message = str(context.get('exception'))
    task_id = ti.task_id
    dag_id = ti.dag_id
    execution_date = ti.execution_date
    error={task_id:error_message,"dag_id":dag_id,"execution_date":execution_date}
    ti.xcom_push(key='error_message', value=error)

    

def process_failure_info(**kwargs):
    """
    Task to pull the error message from XCom and process it (e.g., log it or take corrective actions)
    """
    ti = kwargs['ti']
    error_message = ti.xcom_pull(task_ids='spark_submit_job', key='error_message')
    print(f"Processing failure information: {error_message}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 13),
    'on_failure_callback': failure_callback,
}

dag = DAG(
    'spark_postgres_dag',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
)


spark_job = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark-conn',  
    application='jobs/spark_insert.py', 
    jars="/opt/bitnami/spark/data/postgresql-42.4.4.jar",
    conf={
        "spark.driver.extraClassPath": "/opt/bitnami/spark/data/postgresql-42.4.4.jar",
        "spark.executor.extraClassPath": "/opt/bitnami/spark/data/postgresql-42.4.4.jar"
    },
    on_failure_callback=failure_callback,
    dag=dag,
)

failure_task = PythonOperator(
    task_id='process_failure_info',
    python_callable=process_failure_info,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag,
)

spark_job >> failure_task
