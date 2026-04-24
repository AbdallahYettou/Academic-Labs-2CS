import json
import os
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define the DAG with a daily schedule interval
dag = DAG(
    dag_id='download_rocket_launches',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Download launches
# Executes the curl command and stores the JSON result
download_launches = BashOperator(
    task_id="download_launches",
    bash_command='curl -Lk "https://ll.thespacedevs.com/2.0.0/launch/upcoming" -o /opt/airflow/dags/tmp/launches.json',
    dag=dag,
)

# Task 2: Get pictures (Python function)
def _get_pictures():
    # (1) Create the directory
    os.makedirs('/opt/airflow/dags/tmp/images', exist_ok=True)
    
    # (2) Read the content of the JSON file
    with open('/opt/airflow/dags/tmp/launches.json') as f:
        launches = json.load(f)
        
    # (3 & 4) Retrieve URLs, send HTTP GET, and store images
    for launch in launches.get('results', []):
        image_url = launch.get('image')
        if image_url:
            response = requests.get(image_url)
            # Extract the filename from the URL
            image_filename = image_url.split('/')[-1]
            with open(f'/opt/airflow/dags/tmp/images/{image_filename}', 'wb') as image_file:
                image_file.write(response.content)

# Task 2: Get pictures (Operator)
get_pictures = PythonOperator(
    task_id='get_pictures',
    python_callable=_get_pictures,
    dag=dag,
)

# Task 3: Notify
# Displays the number of images in the directory
notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls -1 /opt/airflow/dags/tmp/images/ | wc -l) images."',
    dag=dag,
)

# Define task dependencies
download_launches >> get_pictures >> notify