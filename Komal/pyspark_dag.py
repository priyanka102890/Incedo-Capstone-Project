from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

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
    'pyspark_csv_processing_dag',
    default_args=default_args,
    description='DAG to read, process, and write CSV using PySpark',
    schedule_interval=timedelta(days=1),
)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("PySpark DAG Example") \
        .getOrCreate()
    return spark

def read_csv_with_pyspark(**kwargs):
    spark = create_spark_session()
    df = spark.read.csv("/home/ubuntu/airflow/airflow_input/customers_table.csv", header=True, inferSchema=True)
    pd_df = df.toPandas()  # Convert to Pandas DataFrame for XCom serialization
    kwargs['ti'].xcom_push(key='dataframe', value=pd_df.to_dict())
    spark.stop()

def process_dataframe_with_pyspark(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='dataframe', task_ids='read_csv_with_pyspark')
    pd_df = pd.DataFrame(df_dict)
    spark = create_spark_session()
    df = spark.createDataFrame(pd_df) 
    filtered_df = df.filter(col("Country") == 'Germany')
    pd_filtered_df = filtered_df.toPandas()  
    kwargs['ti'].xcom_push(key='processed_dataframe', value=pd_filtered_df.to_dict())
    spark.stop()

def write_csv_with_pyspark(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(key='processed_dataframe', task_ids='process_dataframe_with_pyspark')
    pd_df = pd.DataFrame(df_dict)
    spark = create_spark_session()
    df = spark.createDataFrame(pd_df)  

    df.write.csv("/home/ubuntu/airflow/airflow_output/new_customers_table.csv", header=True, mode="overwrite")
    spark.stop()

# Define the tasks
task1 = PythonOperator(
    task_id='read_csv_with_pyspark',
    python_callable=read_csv_with_pyspark,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='process_dataframe_with_pyspark',
    python_callable=process_dataframe_with_pyspark,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='write_csv_with_pyspark',
    python_callable=write_csv_with_pyspark,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3
