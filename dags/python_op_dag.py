from datetime import timedelta, datetime
import random

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'whisker',
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


def generate_num(start, end, ti):
    ti.xcom_push(key='seq', value=random.randint(start, end))


def get_user_name(ti):
    ti.xcom_push(key='user_name', value="Developer")


def print_user_id(ti):
    seq = ti.xcom_pull(task_ids='gen_seq', key='seq')
    user_name = ti.xcom_pull(task_ids='get_username', key='user_name')
    print(f"Hi {user_name}{seq}!")


with DAG(
    dag_id='python_dag',
    description='Python Operator Test',
    default_args=default_args,
    start_date=datetime(2024, 2, 20, 12),
    schedule_interval='@daily'
) as dag:
    get_username_task = PythonOperator(
        task_id='get_username',
        python_callable=get_user_name,
    )

    gen_seq_task = PythonOperator(
        task_id='gen_seq',
        python_callable=generate_num,
        op_kwargs={'start': 1, 'end': 100}
    )

    print_user_id_task = PythonOperator(
        task_id='print_user_id',
        python_callable=print_user_id,
    )

    [get_username_task, gen_seq_task] >> print_user_id_task
