from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import os
from airflow.models import Variable

def f(x): 
    print('Hello! Bitch', x)
    print(os.getcwd())
    print(os.listdir(os.getcwd()))
    print(os.listdir('/'))
    print(os.listdir('/opt'))
    print(Variable.get("github_login", default_var = None))

dag = DAG('test', 
    schedule="@once",
    start_date=datetime.datetime(2024, 9, 17, 0)
    )

function = PythonOperator( 
    task_id='function',
    python_callable=f,
    op_kwargs={'x': 17},
    dag=dag
)

function
