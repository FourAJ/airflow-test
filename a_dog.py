from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 25, 11, 00, tzinfo=timezone(timedelta(hours=3))),
    'retries': 1,
}

with DAG(
        dag_id='a_dog',
        default_args=default_args,
        schedule=None,
        catchup=False,
) as a_dog:
    def first_python_task():
        print("first a_dog task")


    a_dog_task_1 = PythonOperator(
        task_id='a_dog_first_task',
        python_callable=first_python_task,
        dag=a_dog,
    )

    a_dog_task_2 = TriggerDagRunOperator(
        task_id='a_dog_trigger_b_dog_task',
        trigger_dag_id='b_dog',
        trigger_run_id='b_dog_first_task',
        dag=a_dog,
    )

    a_dog_task_3 = PythonOperator(
        task_id='a_dog_third_task',
        python_callable=lambda: print('third a_dog task'),
        dag=a_dog
    )

    a_dog_task_1 >> a_dog_task_2 >> a_dog_task_3
