'''
workflow explaination:

1. elt script would first to trigger for the movement of the data and arrive at the postgre sql database
2. dbt would scan the tables in the database and replicate the copy of data to perform more in-house transformation
3. dbt would then run the test on the transformed data and the flow would terminate if any checkpoint fails
4. the finalized aggregated data would be uploaded to s3 and consume by BI applications for analysis

'''


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import os

#define your project path here
PROJECT_PATH = '/opt/airflow/src'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

def upload_processed_data():
    # in order for the airflow to search for the module, the path has to mount to the source folder
    os.chdir(f'{PROJECT_PATH}')
    import sys
    sys.path.append(f"{PROJECT_PATH}")
    from s3_storage import S3_upload
    from postgres import export_data_to_csv

    file_name = f"{PROJECT_PATH}/data/full_denormed_table.csv"
    export_data_to_csv(table_name='full_denormed_table', file_name=file_name)
    
    with open(file_name, 'rb') as f:
        S3_upload(bucket='data-engineering-arlington-property-sale', 
                 file=f, 
                 filename='full_denormed_table.csv')

with DAG(
    dag_id='Arlington_Property_Sales_Pipeline',
    default_args=default_args,
    description='Orchestrating the process of flows for data collection, transformation, and loading',
    catchup=False,
) as dag:
    
    etl_script = BashOperator(
        task_id='etl_script',
        bash_command=f'cd {PROJECT_PATH} && python3 main.py',
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd /opt/dbt_transformation && dbt run --full-refresh',
    )
    dbt_quality_check = BashOperator(
        task_id='dbt_quality_check',
        bash_command=f'cd /opt/dbt_transformation && dbt test --select fact_sale.sql',
    )

    upload_processed_data = PythonOperator(
        task_id='upload_processed_data',
        python_callable=upload_processed_data
    )

    etl_script >> dbt_run >> dbt_quality_check >> upload_processed_data