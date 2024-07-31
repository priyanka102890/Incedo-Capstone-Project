from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    's3_csv_processing_dag',
    default_args=default_args,
    description='DAG to read, process, and write CSV from/to S3 using pandas',
    schedule_interval=timedelta(days=1),
)


def read_csv_from_s3(**kwargs):
    df = pd.read_csv('/home/ubuntu/airflow/airflow_input/customers_table.csv')
    kwargs['ti'].xcom_push(key='dataframe', value=df.to_dict())

def process_dataframe(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='dataframe', task_ids='read_csv_from_s3')
    df = pd.DataFrame(df_dict)
    country = 'Germany'
    filtered_df = df[df["Country"] == country]
    kwargs['ti'].xcom_push(key='processed_dataframe', value=filtered_df.to_dict())

def write_csv_to_s3(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='processed_dataframe', task_ids='process_dataframe')
    df = pd.DataFrame(df_dict)
    csv_buffer = BytesIO()
    df.to_csv('/home/ubuntu/airflow/airflow_output/new_customers_table.csv')
    

# Define the tasks
task1 = PythonOperator(
    task_id='read_csv_from_s3',
    python_callable=read_csv_from_s3,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='process_dataframe',
    python_callable=process_dataframe,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='write_csv_to_s3',
    python_callable=write_csv_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3
