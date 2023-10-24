from datetime import datetime, timedelta, timezone
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

defaultMainArgs = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 24, 17, 5, tzinfo=timezone(timedelta(hours=3))),
    'retries': 1
}

mainDag = DAG(
    'bigDog',
    default_args=defaultMainArgs,
    schedule=timedelta(minutes=10),
    catchup=False,
)


def createDags():
    current_script_path = os.path.abspath(__file__)
    script_directory = os.path.dirname(current_script_path)
    csv_file_path = os.path.join(script_directory, 'dogs.csv')

    with open(csv_file_path, 'r') as dogs:
        csvReader = csv.reader(dogs)

        for row in csvReader:
            dagName = row[0].strip("'")
            dagOwner = row[1].strip("'")
            dagStartDate = row[2].strip("'")
            dagRetries = int(row[3])
            dagSchedule = int(row[4])
            dagCatchup = bool(row[5])
            dogFile = f"""from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator      

with DAG(
    '{dagName}',
    default_args={{
        'owner': '{dagOwner}',
        'start_date': datetime.strptime('{dagStartDate}', "%Y-%m-%d %H:%M"),
        'retries': {dagRetries},
    }},
    schedule=timedelta(minutes={dagSchedule}),
    catchup={dagCatchup},
    ) as newDag:
        task = PythonOperator(
            task_id='lilTask_1',
            python_callable=lambda: print('ok'),
            dag=newDag
        )
        task2 = PythonOperator(
            task_id='lilTask_2',
            python_callable=lambda: print('ok 2'),
            dag=newDag
        )
        task >> task2
                    """
            with open(f'{script_directory}/{dagName}.py', 'w') as file:
                file.write(dogFile)


mainTask = PythonOperator(
    task_id='bigTask',
    python_callable=createDags,
    dag=mainDag
)

main_task_1 = PythonOperator(
    task_id='main_task_1',
    python_callable=lambda: print('This is the main task 1'),
    dag=mainDag
)

main_task_1 >> mainTask
