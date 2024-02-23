from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'whisker',
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='bash_dag',
    description='Bash Operator Test',
    default_args=default_args,
    start_date=datetime(2024, 2, 20, 12),
    schedule_interval='@daily'
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, {{ var.value.app_name }} !"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo this is the second task!"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo this is the third task!"
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # Task dependency method 3
    # [task2, task3] >> task1
    task1 >> [task2, task3]
