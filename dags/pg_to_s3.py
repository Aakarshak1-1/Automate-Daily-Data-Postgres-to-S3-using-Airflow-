from datetime import datetime, timedelta
import csv
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile

# Default arguments
default_args = {
    'owner': 'aakarshak',
}

# Path to the temporary file (we'll share this path between tasks via XCom)
TEMP_FILE_PATH = "/tmp/orders_{}.csv"

# Task 1: Extract data from Postgres and save it as a CSV file
def extract_data_from_postgres(data_interval_start, data_interval_end, **kwargs):
    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Extract data from 'orders' table between the current execution date and the next execution date
    cursor.execute(
        "SELECT * FROM orders WHERE date >= %s AND date < %s",
        (data_interval_start, data_interval_end)
    )

    # Create a temporary CSV file and write the data to it
    temp_file_path = TEMP_FILE_PATH.format(data_interval_start)
    with open(temp_file_path, mode="w") as temp_file:
        csv_writer = csv.writer(temp_file)
        csv_writer.writerow([desc[0] for desc in cursor.description])  # Write header
        csv_writer.writerows(cursor)  # Write data
        temp_file.flush()
        logging.info(f"Temporary CSV file created: {temp_file_path}")
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    # Return the path of the CSV file (used for passing between tasks via XCom)
    return temp_file_path

# Task 2: Upload the CSV file to S3
def upload_to_s3(data_interval_start, **kwargs):
    # Get the file path from the previous task's output
    ti = kwargs['ti']
    temp_file_path = ti.xcom_pull(task_ids='extract_data_task')

    # Upload the temporary CSV file to S3
    s3_hook = S3Hook(aws_conn_id="s3_connection")
    s3_key = f"orders/{data_interval_start}.csv"
    
    s3_hook.load_file(
        filename=temp_file_path,
        key=s3_key,
        bucket_name="postgres-to-s3-using-airflow",
        replace=True
    )
    
    logging.info(f"Orders data for {data_interval_start} uploaded to S3: s3://postgres-to-s3-using-airflow/{s3_key}")

# Define the DAG
with DAG(
    dag_id="from_postgres_to_s3_v01",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False  # Prevents backfilling of missed runs
) as dag:
    
    # Task 1: Extract data from Postgres and save it as a CSV file
    extract_data_task = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract_data_from_postgres,
        provide_context=True
    )

    # Task 2: Upload the CSV file to S3
    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3_task",
        python_callable=upload_to_s3,
        provide_context=True
    )

    # Set task dependencies
    extract_data_task >> upload_to_s3_task
