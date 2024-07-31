from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'python_air',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
	}

def my_function1(x):
    return x + " in Python."

def my_function2(x):
    return x + " is learning Airflow"

dag = DAG(
    'PythonOps',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval=timedelta(days=1)
)

callf1 = PythonOperator(
    task_id='callf1',
    python_callable = my_function1,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

callf2 = PythonOperator(
    task_id='callf2',
    python_callable = my_function2,
    op_kwargs = {"x" : "Jaadu"},
    dag=dag,
)

callf1 >> callf2
