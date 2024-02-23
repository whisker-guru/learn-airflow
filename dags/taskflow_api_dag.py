from datetime import timedelta, datetime
import random
from airflow.decorators import dag, task

default_args = {
    'owner': 'whisker',
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


@dag(dag_id='taskflow_api_dag',
     description='Python Operator with Taskflow API Test',
     default_args=default_args,
     start_date=datetime(2024, 2, 20, 12),
     schedule_interval='@daily')
def hi_user_etl():
    @task()
    def get_user_name():
        return "Developer"

    @task()
    def generate_num(start, end):
        return random.randint(start, end)

    @task()
    def print_user_id(user_name, seq):
        print(f"Hi {user_name}{seq}!")

    user_name = get_user_name()
    seq = generate_num(0, 100)
    print_user_id(user_name, seq)


greet_dag = hi_user_etl()
