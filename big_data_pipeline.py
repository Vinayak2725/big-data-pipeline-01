from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define default_args to specify default parameters for the DAG
default_args = {
    'owner': 'vinayak',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance with your specified parameters
dag = DAG(
    'big_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,  # Do not catch up on missed runs
    description='Big Data Pipeline',
)

# Task 1: Start PostgreSQL and Hive containers
start_postgresql_hive = BashOperator(
    task_id='start_postgresql_hive',
    bash_command='docker-compose up -d postgres hive',
    dag=dag,
)

# Task 2: Create PostgreSQL tables
create_postgresql_tables = BashOperator(
    task_id='create_postgresql_tables',
    bash_command='python create_postgresql_tables.py',
    dag=dag,
)

# Task 3: Create Hive tables
create_external_Hive_tables = BashOperator(
    task_id='create_external_Hive_tables',
    bash_command='python create_external_Hive_tables.py',
    dag=dag,
)

# Task 4: ETL data from PostgreSQL to Hive
etl_data_to_hive = BashOperator(
    task_id='etl_data_to_hive',
    bash_command='python etl_data_to_hive.py',
    dag=dag,
)

# Task 5: Create new Hive tables
create_new_hive_tables = BashOperator(
    task_id='create_new_hive_tables',
    bash_command='python create_new_hive_tables.py',
    dag=dag,
)

# Task 6: Populate new Hive tables
populate_new_hive_tables = BashOperator(
    task_id='populate_new_hive_tables',
    bash_command='python populate_new_hive_tables.py',
    dag=dag,
)

# Task 7: Save query results to CSV files
save_qry_output_to_csv = BashOperator(
    task_id='save_qry_output_to_csv',
    bash_command='python save_qry_output_to_csv.py',
    dag=dag,
)

# Set up task dependencies
start_postgresql_hive >> create_postgresql_tables
create_postgresql_tables >> create_external_Hive_tables
create_external_Hive_tables >> etl_data_to_hive
etl_data_to_hive >> create_new_hive_tables
create_new_hive_tables >> populate_new_hive_tables
populate_new_hive_tables >> save_qry_output_to_csv