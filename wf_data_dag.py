from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import boto3
from dbfread import DBF
import pandas as pd

import os 
from io import BytesIO, StringIO
from datetime import datetime, timedelta

# Configuring Spaces Client
session = boto3.session.Session()
client = session.client('s3',
                        region_name='nyc3',                                     # Replace with correct region
                        endpoint_url='https://nyc3.digitaloceanspaces.com',     # Replace with correct endpoint
                        aws_access_key_id=os.getenv('SPACES_KEY'),              # Replace with access key
                        aws_secret_access_key=os.getenv('SPACES_SECRET'))       # Replace with secret key

s3 = session.resource('s3',
                        region_name='nyc3',                                     # Replace with correct region
                        endpoint_url='https://nyc3.digitaloceanspaces.com',     # Replace with correct endpoint
                        aws_access_key_id=os.getenv('SPACES_KEY'),              # Replace with access key
                        aws_secret_access_key=os.getenv('SPACES_SECRET'))       # Replace with secret key

def_args = {
    'owner':'airflow',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'email': ['ivanedric@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=20)
    }

with DAG('wf_data_dag',
    default_args=def_args,
    schedule_interval='@weekly',        # '0 0 * * 0': Runs weekly on Sunday mornings 
    catchup=False,) as dag:
    
    def _DBG_to_CSV_convert(**kwargs):
        ''' Converts DBF to CSV and uploads back to Ocean Spaces '''
        # Read from DigitalOcean Space
        obj = client.get_object(kwargs['Bucket'], kwargs['Key'])
        dbf = DBF(BytesIO(obj['Body'].read()), encoding = "ISO-8859-1")
        df = pd.DataFrame(iter(dbf))

        # Write back to DigitalOcean Space
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        client.put_object(kwargs['Bucket'], kwargs['Target_Key'], Body=csv_buffer.getvalue())

        ''' We  can also delete the original DBF from Digital Ocean to save costs '''
    
    def _generate_reports(**kwargs):
        ''' Generates needed reports for one branch'''
        # Get CSVs from DigitalOcean Space
        sdet_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_SDET'])
        sdet_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        sls_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_SLS'])
        sls_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        pages_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_PAGES'])
        pages_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        menu_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_MENU'])
        menu_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        pagetype_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_PAGETYPE'])
        pagetype_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        revcent_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_REVCENT'])
        revcent_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        tipopag_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_TIPOPAG'])
        tipopag_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')

        

    # DBF Conversion Jobs
    # NOTE: Must replace Buckets and Keys with correct Buckets + Keys
    SDET_DBF_to_CSV_convert = PythonOperator(
        task_id = 'SDET_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )

    SLS_DBF_to_CSV_convert = PythonOperator(
        task_id = 'SLS_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )

    PAGES_DBF_to_CSV_convert = PythonOperator(
        task_id = 'PAGES_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )

    MENU_DBF_to_CSV_convert = PythonOperator(
        task_id = 'MENU_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )

    PAGETYPE_DBF_to_CSV_convert = PythonOperator(
        task_id = 'PAGETYPE_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )

    REVCENT_DBF_to_CSV_convert = PythonOperator(
        task_id = 'REVCENT_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )
    
    TIPOPAG_DBF_to_CSV_convert = PythonOperator(
        task_id = 'TIPOPAG_DBF_to_CSV_convert',
        python_callable = _DBG_to_CSV_convert,
        op_kwargs={'Bucket': 'name_of_bucket', 'Key': 'name_of_key', 'Target_Key': 'name_of_target'}
    )

    generate_reports = BranchPythonOperator(
        task_id = 'generate_reports',
        python_callable = _generate_reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key'}
    )