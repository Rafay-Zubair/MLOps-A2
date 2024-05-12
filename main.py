import requests
from bs4 import BeautifulSoup
import pendulum
import datetime
import os
import pandas as pd
# import tqdm
# from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
# from datetime import datetime

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']
links = [[], []] # one list item for each source...
df_links = None

def extract():
    df_links = None

    for i, source in enumerate(sources):
        data = requests.get(source)
        html_data = BeautifulSoup(data.text, 'html.parser')
        links[i] = [j['href'] for j in html_data.find_all('a', href=True)]

    df_links = pd.DataFrame(columns=['link', 'title', 'description'])
    for i in links:
        for idx, j in enumerate(i):
            if idx == 30:
                break

            try:
                j_data = requests.get(j)
                if j_data.status_code != 200:
                    continue

                j_soup = BeautifulSoup(j_data.text, 'html.parser')

                title = j_soup.find('title')
                title = title.get_text()
                if title == None or title == '':
                    continue

                description = " ".join([p.get_text() for p in j_soup.find_all('p')])
                if description == None or description == '':
                    continue

                df_links = df_links.append(pd.DataFrame({'link': [j], 'title': [title], 'description': [description]}), ignore_index=True)
            except:
                continue

    Variable.set("df_links", df_links.to_json(), serialize_json=True)

# END of Extract Function


def transform(**kwargs):
    print("Transformation")

def load(**kwargs):
    df_links = Variable.get("df_links", deserialize_json=True)

    # type checking
    if type(df_links) != pd.DataFrame:
        df_links = pd.read_json(df_links)

    # create csv dataset
    df_links.to_csv('/opt/airflow/dags/data/link_data.csv', index=False)

    # version the dataset and push to dvc
    dvc_command = ['cd "E:/Semester VIII/MLOps/airflow/dags"', 'dvc add ./data/link_data.csv', 'dvc push']
    git_command = ['cd "E:/Semester VIII/MLOps/airflow/dags"', 'git add .', 'git commit -m "New Dataset"', 'git push origin master'] 

    os.system(' && '.join(dvc_command))
    os.system(' && '.join(git_command))

# END of Load Function

default_args = {
    'owner' : 'airflow-demo'
}


@dag(
    dag_id = 'mlops-a2-dag',
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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