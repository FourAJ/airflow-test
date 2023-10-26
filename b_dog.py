from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.ssh.hooks.ssh import SSHHook
# from airflow.providers.ssh.operators.ssh import SSHOperator
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

    # ssh_hook = SSHHook(
    #     ssh_conn_id='',
    #     remote_host='',
    #     username='',
    #     password=''
    # )

    # def b_dog_ssh_hook():
    #     ...
    #     ssh_hook.get_conn().exec_command('shuffle <3')
    #     ...

    # b_dog_task_4 = PythonOperator(
    #     task_id='b_dog_python_shh_task',
    #     python_callable=b_dog_ssh_hook,
    #     dag=b_dog,
    # )

    # b_dog_task_3 = SSHOperator(
    #     task_id='b_dog_ssh_task',
    #     ssh_hook=ssh_hook,
    #     command='...',
    #     dag=b_dog,
    # )

    b_dog_task_1 >> b_dog_task_2
