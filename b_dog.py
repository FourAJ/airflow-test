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
    dag_id='b_dog',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as b_dog:
    # b_dog_task_1 = ExternalTaskSensor(
    #     task_id='b_dog_first_task',
    #     external_dag_id='a_dog',
    #     external_task_id='a_dog_third_task',
    #     dag=b_dog
    # )

    b_dog_task_1 = PythonOperator(
        task_id='b_dog_first_task',
        python_callable=lambda: print('first b_dog task'),
        dag=b_dog,
    )

    b_dog_task_2 = PythonOperator(
        task_id='b_dog_second_task',
        python_callable=lambda: print('second b_dog task'),
        dag=b_dog,
    )

    b_dog_task_1 >> b_dog_task_2