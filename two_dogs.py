import time
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 25, 16, 51, tzinfo=timezone(timedelta(hours=3))),
    'retries': 1,
}

with DAG(
    dag_id='three_a_dog',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    catchup=False,
) as a_dog:
    def a_dog_func():
        time.sleep(60)
        print('first b_dog task access')
    a_dog_task_1 = PythonOperator(
        task_id='three_a_dog_first_task',
        python_callable=a_dog_func,
        dag=a_dog,
    )

with DAG(
    dag_id='three_b_dog',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    catchup=False,
) as b_dog:
    def b_dog_func():
        time.sleep(30)
        print('first b_dog task access')
    b_dog_task_1 = PythonOperator(
        task_id='three_b_dog_first_task',
        python_callable=b_dog_func,
        dag=b_dog,
    )

with DAG(
    dag_id='three_c_dog',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    catchup=False,
) as c_dog:
    sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        poke_interval=1,
        timeout=90,
        soft_fail=False,
        retries=2,
        external_task_id='three_a_dog_first_task',
        external_dag_id='three_a_dog',
        dag=c_dog
    )
    sensor_b = ExternalTaskSensor(
        task_id='external_task_sensor_b',
        poke_interval=1,
        timeout=90,
        soft_fail=False,
        retries=2,
        external_task_id='three_b_dog_first_task',
        external_dag_id='three_b_dog',
        dag=c_dog
    )
    task_b_dog = EmptyOperator(
        task_id='task_c',
        dag=c_dog
    )
    [sensor_a, sensor_b] >> task_b_dog

