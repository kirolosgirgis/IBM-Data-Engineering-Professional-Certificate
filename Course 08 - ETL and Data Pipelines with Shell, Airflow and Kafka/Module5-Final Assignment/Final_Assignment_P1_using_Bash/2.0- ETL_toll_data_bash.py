# Import the libraries

# Time library
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# Define DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner':'kirolos',
    'start_date': days_ago(0),
    'email':['k.yossif@aucegypt.edu'],
    'email_on_failure': True,
    'email_on retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG

dag = DAG(
    'ETL_toll_data_bash',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1)
)

# Create a task extract_transform_load in the ETL_toll_data.py to call the shell script.

extract_transform_load = BashOperator(
    task_id = 'extract_transform_load',
    bash_command = '/home/project/airflow/dags/Extract_Transform_data.sh',
    dag=dag,
)

# Task pipeline
extract_transform_load