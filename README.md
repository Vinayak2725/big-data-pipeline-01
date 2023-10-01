# big-data-pipeline-01
Postgres to Hive data pipeline - using Big data frameworks ( Apache Spark, Apache Sqoop) 


Automated Execution with Airflow DAG

Step 1 : 
Adjust file paths, script names, and configurations as needed for your specific environment.

1. Defining PostgreSQL Connection Parameters
File: create_postgresql_tables.py
Replace "yourdb", "yourusername", and "yourpassword" with your PostgreSQL connection details:
python
Copy code
db_params = {
    "dbname": "yourdb",
    "user": "yourusername",
    "password": "yourpassword",
    "host": "localhost",
    "port": "5432",
}

2. Define Hive Connection Parameters
Files: create_external_Hive_tables.py, create_new_hive_tables.py, populate_new_hive_tables.py, save_qry_output_to_csv.py
Replace "your_hive_username" and "localhost" with your Hive connection details:
python
Copy code
hive_host = "localhost"
hive_port = 10000
hive_username = "your_hive_username"

3. Update output dir path in "save_qry_output_to_csv.py"
output_directory = '/path/to/output/directory/'

Step 2 : 
Make sure you have Airflow configured and your DAG script is placed in the appropriate directory.

Step 3 : 
Open the Airflow web UI and trigger the DAG named "big_data_pipeline".
