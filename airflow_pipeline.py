"""Understanding"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_etl_pipeline',
    default_args=default_args,
    description='A sample ETL pipeline',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=days_ago(1),
    catchup=False,
)

# Define Python functions for ETL steps
def extract_data():
    """Extract data from a source (simulated with sample data)"""
    # In reality, this could connect to a database or API
    data = {
        'id': [1, 2, 3, 4],
        'name': ['John', 'Jane', 'Bob', 'Alice'],
        'sales': [100, 200, 150, 175]
    }
    df = pd.DataFrame(data)
    df.to_csv('/tmp/extracted_data.csv', index=False)
    print("Data extracted successfully")

def transform_data():
    """Transform the extracted data"""
    df = pd.read_csv('/tmp/extracted_data.csv')
    # Sample transformation: calculate sales with 10% tax
    df['sales_with_tax'] = df['sales'] * 1.1
    df.to_csv('/tmp/transformed_data.csv', index=False)
    print("Data transformed successfully")

def load_data():
    """Load the transformed data"""
    # In reality, this could load to a database
    df = pd.read_csv('/tmp/transformed_data.csv')
    print(f"Loaded data:\n{df}")

# Define the tasks/operators
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# Add a simple bash command to show pipeline completion
complete_task = BashOperator(
    task_id='complete',
    bash_command='echo "Pipeline completed at $(date)"',
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> complete_task
