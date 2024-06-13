# Import the libraries

# Time library
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# Task 1.1 - Define DAG arguments

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

# Task 1.2 - Define the DAG

dag = DAG(
    'ETL_python',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1)
)

# Task 1.3 - Create a shell script Extract_Transform_data.sh

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzvf /home/project/airflow/dags/tolldata.tgz',
    dag=dag,
)

# Task 1.4 -  Update the shell script to add a command to extract data from csv file

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -f1-4 -d"," /home/project/airflow/dags/vehicle-data.csv > \
    /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)

# Task 1.5 - Update the shell script to add a command to extract data from tsv file

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 /home/project/airflow/dags/tollplaza-data.tsv | tr "\t" "," > \
    /home/project/airflow/dags/tsv_data.csv',
    dag=dag,
)

# Task 1.6 - Update the shell script to add a command to extract data from fixed width file

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c59-67 /home/project/airflow/dags/payment-data.txt | tr " " "," > \
    /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag,
)

# Task 1.7 - Update the shell script to add a command to consolidate data extracted from previous tasks

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d"," /home/project/airflow/dags/csv_data.csv \
    /home/project/airflow/dags/tsv_data.csv \
    /home/project/airflow/dags/fixed_width_data.csv \
    > /home/project/airflow/dags/extracted_data.csv',
    dag=dag,
)

# Task 1.8 - Update the shell script to add a command to Transform and load the data

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'tr "[:lower:]" "[:upper:]" < /home/project/airflow/dags/extracted_data.csv \
    > /home/project/airflow/dags/transformed_data.csv',
    dag=dag,
)

# Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data