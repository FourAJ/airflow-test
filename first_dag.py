from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 24, 16, 50),
    'retries': 1,
}

with DAG(
    'first_dog',
    default_args=default_args,
    schedule=timedelta(minutes=1),
    catchup=False,
) as dag:
    def first_python_task():
        print("first dog task")


    def second_python_task():
        print("second dog task")


    task1 = PythonOperator(
        task_id='first_task',
        python_callable=first_python_task,
        dag=dag,
    )

    task2 = PythonOperator(
        task_id='second_task',
        python_callable=second_python_task,
        dag=dag,
    )

    task1 >> task2
