from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 25, 11, 00, tzinfo=timezone(timedelta(hours=3))),
    'retries': 1,
}

a_dog = DAG(
    dag_id='two_a_dog',
    default_args=default_args,
    schedule=None,
    catchup=False,
)

b_dog = DAG(
    dag_id='two_b_dog',
    default_args=default_args,
    schedule=None,
    catchup=False,
)


a_dog_task_1 = PythonOperator(
    task_id='two_a_dog_first_task',
    python_callable=lambda: print('first a_dog task'),
    dag=a_dog
)

b_dog_task_1 = PythonOperator(
    task_id='two_b_dog_first_task',
    python_callable=lambda: print('first b_dog task'),
    dag=b_dog
)

a_dog_task_1 >> b_dog_task_1