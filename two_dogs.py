import time
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 26, 12, 57, tzinfo=timezone(timedelta(hours=3))),
    'retries': 1,
}

with DAG(
    dag_id='a_new_dog_a',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    # schedule=None,
    catchup=False,
) as a_dog:

    a_dog_task_1 = PythonOperator(
        task_id='start',
        python_callable=lambda: print('start'),
        dag=a_dog,
    )

    a_dog_task_2 = PythonOperator(
        task_id='end',
        python_callable=lambda: print('end'),
        dag=a_dog,
    )
    a_dog_task_1 >> a_dog_task_2

with DAG(
    dag_id='a_new_dog_b',
    default_args=default_args,
    schedule=timedelta(minutes=1),
    catchup=False,
) as b_dog:
    sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        poke_interval=5,
        soft_fail=False,
        timeout=60,
        retries=10,
        execution_delta=timedelta(seconds=30),
        external_task_id='end',
        external_dag_id='a_new_dog_a',
        dag=b_dog
    )
    task_b_dog = EmptyOperator(
        task_id='task_b',
        dag=b_dog
    )
    sensor_a >> task_b_dog

