from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

default_args = {
    'owner': 'whisker',
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
        dag_id='papermill_dag',
        description='Papermill Operator Test',
        default_args=default_args,
        start_date=datetime(2024, 2, 20, 12),
        schedule_interval='@once'
) as dag:
    get_vars_task = PapermillOperator(
        task_id='get_vars_task',
        input_nb='/Users/yihuiwang/learn-workspace/whisker-git/airflow-learning/jupyter/get_params_and_vars.ipynb',
        output_nb='/Users/yihuiwang/learn-workspace/whisker-git/airflow-learning/jupyter/get_params_and_vars_out.ipynb',
        parameters={'greeting': 'Hi, ', "execution_date": '{{ execution_date }}'}
    )

    print_xcom_task = PapermillOperator(
        task_id='print_xcom_task',
        input_nb='/Users/yihuiwang/learn-workspace/whisker-git/airflow-learning/jupyter/print_xcom.ipynb',
        output_nb='/Users/yihuiwang/learn-workspace/whisker-git/airflow-learning/jupyter/print_xcom_out.ipynb',
        parameters={"execution_date": '{{ execution_date }}'}
    )

    get_vars_task >> print_xcom_task
