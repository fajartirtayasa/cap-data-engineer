from proj_hive_getdata import get_data
from proj_hive_transformation import read_transform_load

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator

default_args = {
        'owner' : 'fatir',
        'retries' : 1,
        'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_green-taxi',
    description='DAG NYC Green Taxi Trip',
    start_date=datetime(2023,5,23),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'get_data',
        python_callable = get_data
    )
    task2 = BashOperator(
        task_id = 'copy_to_hdfs',
        bash_command = 'hadoop fs -copyFromLocal -f /home/fatir/green-taxi-data/*.parquet /user/fatir/taxi-data'
    )
    task3 = HiveOperator(
        task_id = 'staging_in_hive',
        hql = open('/mnt/c/Users/user/Documents/airflow/dags/proj_hive_hql.sql', 'r').read(),
        hive_cli_conn_id = 'hive_default'
    )
    task4 = PythonOperator(
        task_id = 'read_transform_load',
        python_callable = read_transform_load
    )

    task1 >> task2 >> task3 >> task4