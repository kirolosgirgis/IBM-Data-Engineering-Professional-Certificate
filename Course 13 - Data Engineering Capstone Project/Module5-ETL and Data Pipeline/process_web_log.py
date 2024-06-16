# Import the libraries

# Time library
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# Task 1 - Define DAG arguments
default_args = {
    'owner':'Kirolos',
    'start_date': days_ago(0),
    'email':'k.yossif@aucegypt.edu'
}

# Task 2 - Define the DAG
dag = DAG(
    'process_web_log',
    default_args = default_args,
    description = 'Process Web Log',
    schedule_interval = timedelta(days=1)
)

# Task 3 -  Extract Data from txt file
extract_data = BashOperator(
    task_id = 'extract_data',
    bash_command = 'cut -f1 -d" " /home/project/airflow/dags/capstone/accesslog.txt > \
    /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag,
)

# Task 4 -  Transform Data 
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'grep -vw "198.46.149.143" /home/project/airflow/dags/capstone/extracted_data.txt > \
    /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)

# Task 5 -  Load Data 
load_data = BashOperator(
    task_id = 'load_data',
    bash_command = 'tar -zcvf /home/project/airflow/dags/capstone/weblog.tar \
    /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)

# Task 6 - Define pipeline
extract_data >> transform_data >> load_data
