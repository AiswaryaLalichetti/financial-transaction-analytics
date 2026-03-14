from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'ownername',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

AWS_ACCESS_KEY = 'YOUR_AWS_ACCESS_KEY'
AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY'
REGION = 'us-east-1'
GLUE_JOB_NAME = 'fintech-transform-transactions'

def trigger_glue_job():
    client = boto3.client(
        'glue',
        region_name=REGION,
        aws_access_key_id='YOUR_AWS_ACCESS_KEY',
        aws_secret_access_key='YOUR_AWS_SECRET_KEY'
    )
    response = client.start_job_run(JobName=GLUE_JOB_NAME)
    print("Glue job started: " + response['JobRunId'])
    return response['JobRunId']

def check_glue_job(**context):
    job_run_id = context['task_instance'].xcom_pull(task_ids='trigger_glue_job')
    client = boto3.client(
        'glue',
        region_name=REGION,
        aws_access_key_id='AWS_ACCESS_KEY',
        aws_secret_access_key='AWS_SECRET_KEY'
    )
    response = client.get_job_run(
        JobName=GLUE_JOB_NAME,
        RunId=job_run_id
    )
    status = response['JobRun']['JobRunState']
    print("Glue job status: " + status)
    if status == 'FAILED':
        raise Exception("Glue job failed!")
    return status

with DAG(
    'fintech_pipeline',
    default_args=default_args,
    description='Financial Transaction Analytics Pipeline',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fintech', 'banking', 'etl']
) as dag:

    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo Starting Fintech Pipeline'
    )

    trigger_glue = PythonOperator(
        task_id='trigger_glue_job',
        python_callable=trigger_glue_job
    )

    check_glue = PythonOperator(
        task_id='check_glue_status',
        python_callable=check_glue_job
    )
  
    end = BashOperator(
        task_id='end_pipeline',
        bash_command='echo Fintech Pipeline completed successfully'
    )

    start >> trigger_glue >> check_glue >> end