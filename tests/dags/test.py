# my_first_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    'hello_world_dag',
    default_args={
        'retries': 1,
    },
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    t1 >> t2