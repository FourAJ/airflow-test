from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    sensor_a = ExternalTaskSensor(
        task_id='wait_for_a',
        external_dag_id='two_a_dog',
        external_task_id='two_a_dog_first_task',
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
        dag=b_dog,
    )
    task_b_dog = EmptyOperator(
        task_id='task_b',
        dag=b_dog
    )
    sensor_a >> task_b_dog

