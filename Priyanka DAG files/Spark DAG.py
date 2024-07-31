from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_csv_processing_dag',
    default_args=default_args,
    description='DAG to read, filter, and save CSV locally using PySpark',
    schedule_interval=timedelta(days=1),
)

def read_csv_from_local(**kwargs):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("AirflowPySpark") \
        .getOrCreate()

    # Define the path to the input CSV file
    input_path = '/home/ubuntu/airflow/airflow_input/customers_table.csv'

    # Read CSV from local path
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Push DataFrame to XCom as a pandas DataFrame
    kwargs['ti'].xcom_push(key='raw_dataframe', value=df.toPandas().to_dict())

    # Stop SparkSession
    spark.stop()

def filter_dataframe(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='raw_dataframe', task_ids='read_csv_from_local')
    
    # Convert dictionary back to pandas DataFrame
    import pandas as pd
    df = pd.DataFrame(df_dict)

    # Apply filter to the DataFrame
    filtered_df = df[df["Country"] == 'Germany']

    # Push the filtered DataFrame to XCom
    kwargs['ti'].xcom_push(key='filtered_dataframe', value=filtered_df.to_dict())

def write_csv_to_local(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='filtered_dataframe', task_ids='filter_dataframe')
    
    # Convert dictionary back to pandas DataFrame
    import pandas as pd
    df = pd.DataFrame(df_dict)

    # Define the path to the output CSV file
    output_path = '/home/ubuntu/airflow/airflow_output/customer_table_output.csv'
    
    # Write the filtered DataFrame to a new CSV file
    df.to_csv(output_path, index=False)

# Define the tasks
task1 = PythonOperator(
    task_id='read_csv_from_local',
    python_callable=read_csv_from_local,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='filter_dataframe',
    python_callable=filter_dataframe,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='write_csv_to_local',
    python_callable=write_csv_to_local,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3
