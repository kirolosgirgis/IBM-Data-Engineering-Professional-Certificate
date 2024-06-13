
# unzip data
tar -xzvf /home/project/airflow/dags/tolldata.tgz

# extract data from csv file

cut -f1-4 -d"," /home/project/airflow/dags/vehicle-data.csv > \
    /home/project/airflow/dags/csv_data.csv

# extract data from tsv file

cut -f5-7 /home/project/airflow/dags/tollplaza-data.tsv | tr "\t" ","> \
    /home/project/airflow/dags/tsv_data.csv

# extract data from fixed width file

cut -c59-67 /home/project/airflow/dags/payment-data.txt | tr " " "," \
    /home/project/airflow/dags/fixed_width_data_.csv

# consolidate data extracted from previous tasks

paste -d","/home/project/airflow/dags/csv_data.csv \
    /home/project/airflow/dags/tsv_data.csv \
    /home/project/airflow/dags/fixed_width_data_.csv \
    > /home/project/airflow/dags/extracted_data.csv

# Transform and load the data

tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted_data.csv \
    /home/project/airflow/dags/transformed_data.csv