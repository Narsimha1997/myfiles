from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import boto3
import os
access_key='AKIA4TV5BGTQLVVEUD4I'
secret_access_key= "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"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27, 0,0,0)  
 
}

dag = DAG(
    'Start_Instance',
    default_args=default_args,
    schedule_interval=None
)

def startInstance(**kwargs):
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    resource_ec2 = boto3.client('ec2',aws_access_key_id= access_key,aws_secret_access_key= secret_access_key)
    resource_ec2.start_instances(InstanceIds=['i-0415ee2081ad8c6c0'])

start = EmptyOperator(task_id="Starting_Task", dag=dag)

starting = PythonOperator(task_id="Starting_EC2_Instance", dag=dag, python_callable=startInstance)

end = EmptyOperator(task_id="End_Task", dag=dag)

start >> starting  >> end
