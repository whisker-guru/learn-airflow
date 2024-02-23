from datetime import timedelta, datetime

from airflow.decorators import dag, task

# Ref: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html#simple-mapping
default_args = {
    'owner': 'whisker',
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

@dag(dag_id='dynamic_task',
     description='Dynamic Task Test',
     default_args=default_args,
     start_date=datetime(2024, 2, 20, 12),
     schedule_interval='@daily')
def hi_user_etl():

    @task()
    def make_name_list():
        return ["Tester1","Tester2","Tester3"]

    @task()
    def print_name(name):
        print(f"Hi: {name}.")
    print_name.expand(name=make_name_list())

hi_user_etl()
