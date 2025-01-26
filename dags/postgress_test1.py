import psycopg2
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow.utils
import airflow.utils.dates
import os
import polars as pl


dag=DAG(
    dag_id="postgress_test",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1)
)

# Connection details
# host = "postgres_db_service"
# port = "5432"
# database = "transaction_records"
# user = "Mano"
# password = "Mano"


def test_connection():
    host = "postgres_db_service"
    port = "5432"
    database = "transaction_records"
    user = "Mano"
    password = "Mano"
# Try to establish a connection
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
        print("Connection successful!")
        
        # Close the connection after testing
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

test_conn=PythonOperator(
    task_id="test_connection",
    python_callable=test_connection,
    dag=dag
)


test_conn