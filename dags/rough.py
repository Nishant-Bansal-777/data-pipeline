from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG('print_something',
          schedule_interval=None,
          start_date=datetime(2024, 1, 1),
          catchup=False)

# Define the Python function to print something
def print_message():
    with open('p.txt', 'w') as f:
        f.write('Printing something from a PythonOperator with a lambda function!')
    print("Printing something from a PythonOperator with a lambda function!")

# Define a PythonOperator using a lambda function
print_task = PythonOperator(
    task_id='print_task',
    python_callable=print_message,
    dag=dag
)

# Set the task dependency
print_task