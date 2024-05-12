import requests
from bs4 import BeautifulSoup
import pendulum
import datetime
import os
# from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
# from datetime import datetime

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract():
    reqs = requests.get(sources[0])
    soup = BeautifulSoup(reqs.text, 'html.parser')
    urls = []
    for link in soup.find_all('a'):
        print(link.get('href'))

def transform():
    print("Transformation")

def load():
    print("Loading")

"""
for source in sources:
    extract(source)
    transform()
    load()
"""

default_args = {
    'owner' : 'airflow-demo'
}

# dag = DAG(
#     'mlops-dag',
#     default_args=default_args,
#     description='A simple '
# )

@dag(
    dag_id = 'mlops-dag',
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    description='A simple demo of airflow'
)

def tasks():

    task1 = PythonOperator(
        task_id = "Task_1",
        python_callable = extract,
        # dag = dag
    )

    task2 = PythonOperator(
        task_id = "Task_2",
        python_callable = transform,
        # dag=dag
    )

    task3 = PythonOperator(
        task_id = "Task_3",
        python_callable = load,
        # dag=dag
    )

    task1 >> task2 >> task3

dag = tasks()