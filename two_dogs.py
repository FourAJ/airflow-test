from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 25, 11, 00, tzinfo=timezone(timedelta(hours=3))),
    'retries': 1,
}

with DAG(
    dag_id='two_a_dog',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as a_dog:
    a_dog_task_1 = PythonOperator(
        task_id='two_a_dog_first_task',
        python_callable=lambda: print('first a_dog task'),
        dag=a_dog,
    )

with DAG(
    dag_id='two_b_dog',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as b_dog:
    b_dog_task_1 = ExternalTaskSensor(
        task_id='two_b_dog_first_task',
        external_dag_id='two_a_dog',
        external_task_id='two_a_dog_first_task',
        mode='poke',
        timeout=60,
        poke_interval=1,
        dag=b_dog,
    )

