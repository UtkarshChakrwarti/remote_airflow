# Utkarsh Chakrwarti
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Define the DAG
with DAG(
    'hello_world_dag_3.0',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule='@daily',
    catchup=False,
    tags=['example', 'tutorial'],
) as dag:
    
    def print_hello():
        """Print hello message"""
        print('Hello from Airflow!')
        return 'Hello World!'
    
    def print_date():
        """Print current date"""
        print(f'Current date: {datetime.now()}')
        return f'Date: {datetime.now()}'
    
    def print_goodbye():
        """Print goodbye message"""
        print('Goodbye from Airflow!')
        return 'Goodbye!'
    
    # Define tasks
    task_1 = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )
    
    task_2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )
    
    task_3 = PythonOperator(
        task_id='say_goodbye',
        python_callable=print_goodbye,
    )
    
    # Set task dependencies
    task_1 >> task_2 >> task_3
