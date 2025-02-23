'''
workflow explaination:

1. elt script would first to trigger for the movement of the data and arrive at the postgre sql database
2. dbt would scan the tables in the database and replicate the copy of data to perform more in-house transformation
3. dbt would then run the test on the transformed data and the flow would terminate if any checkpoint fails
4. the finalized aggregated data would be uploaded to s3 and consume by BI applications for analysis

'''


from datetime import timedelta
import pendulum
from airflow import DAG
import configparser
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
import os

#define your project path here
PROJECT_PATH = '/home/binchen4568/Arlington-Property-Sales'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def upload_processed_data():
    # in order for the airflow to search for the module, the path has to mount to the source folder
    os.chdir(f'{PROJECT_PATH}/src')
    import sys
    sys.path.append(f"{PROJECT_PATH}/src")
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
    start_date=pendulum.datetime(2025, 2, 23, tz="UTC"),
    schedule_interval='@weekly',
    catchup=False,
) as dag:
    
    etl_script = BashOperator(
        task_id='etl_script',
        bash_command=f'cd {PROJECT_PATH} && python3 src/main.py',
    )
    
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=f'cd {PROJECT_PATH}/dbt_transformation && dbt run --models staging full_denormed_table',
    )
    @task.bash
    def dbt_quality_check():
        try:
            return f'''
            cd {PROJECT_PATH}/dbt_transformation && \
            if ! dbt test --select staging full_denormed_table; then
                echo "DBT tests failed! Check the logs for details."
                exit 1
            fi
            '''
        except Exception as e:
            raise AirflowFailException(f"DBT tests failed! Check the logs for details. Error: {e}")

    upload_processed_data = PythonOperator(
        task_id='upload_processed_data',
        python_callable=upload_processed_data
    )

    etl_script >> run_dbt >> dbt_quality_check() >> upload_processed_data


