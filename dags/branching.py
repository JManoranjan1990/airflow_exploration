import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
import airflow.utils
import airflow.utils.dates
import os
import polars as pl
from airquality.aq import download_air_quality_index


dag=DAG(
    dag_id="branching_python",
    default_args={"owner":"Manoranjan","catchup":False},
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly"
)

def select_branch(input_path,**context):
    # print(input_path),
    print(type(input_path)),
    if os.path.exists(input_path):
        context["ti"].xcom_push(key="input_path",value=input_path)
        return "read_file"
    else:
        return "dummy_task"
    
def read_file(output_path,**context):
    ti = context['ti']
    input_path = ti.xcom_pull(task_ids='Select_branch', key='input_path')
    print(f"x_com value pulled is:{input_path}")
    es=context['execution_date'].strftime('%Y-%m-%d-%H-%M')
    data=pl.read_csv(input_path)
    data.write_parquet(output_path+es)
    
branch=BranchPythonOperator(
    task_id="Select_branch",
    python_callable=select_branch,
    op_kwargs={"input_path":"/opt/airflow/data/branching/airquality.csv"},
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag
    )


read_file=PythonOperator(
    task_id="read_file",
    python_callable=read_file,
    op_kwargs={"output_path":"/opt/airflow/data/branching/output"},
    dag=dag
    # print("reading file")
)



air_quality_index=PythonOperator(
    task_id="download_air_quality_index",
    python_callable=download_air_quality_index,
    op_kwargs={"output_path":"/opt/airflow/data/branching/output/file"},
    trigger_rule = "one_success",
    dag=dag
)
dummy_task=DummyOperator(
    task_id="dummy_task",dag=dag
    # print("reading file")
)

end_task=DummyOperator(
    task_id="end_task",dag=dag
)



branch >>[read_file]
branch>>dummy_task
[dummy_task,read_file]>> air_quality_index>> end_task
