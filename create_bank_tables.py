from datetime import datetime
from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator

default_args = {
    'owner': 'bigdata_devops',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 24),
    'retries': 1,
}

dag = DAG(
    'create_bank_iceberg_tables_dag',
    default_args=default_args,
    description='Create bank Iceberg tables via Spark и Livy',
    schedule=None,
    catchup=False,
    tags=['spark', 'iceberg', 'livy'],
)

create_tables_task = LivyOperator(
    task_id='create_iceberg_tables',
    file='hdfs://master.hadoop.hse.edu:9820/user/spark/create_iceberg_tables.py',
    livy_conn_id='livy',
    polling_interval=1,
    conf={},
    py_files=[],
    driver_memory='512m',
    executor_memory='512m',
    num_executors=1,
    dag=dag,
)
