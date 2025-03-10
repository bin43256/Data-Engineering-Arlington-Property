'''
workflow explaination:

1. elt script would first to trigger for the movement of the data and arrive at the postgre sql database
2. dbt would scan the tables in the database and replicate the copy of data to perform more in-house transformation
3. dbt would then run the test on the transformed data and the flow would terminate if any checkpoint fails
4. the finalized aggregated data would be uploaded to s3 and consume by BI applications for analysis

'''


from airflow import DAG
from airflow.operators.bash import BashOperator

#define project path here
PROJECT_PATH = '/opt/airflow/src'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

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

    duckdb_migration = BashOperator(
        task_id='migrate_to_duckdb',
        bash_command=f'cd {PROJECT_PATH} && python3 duckdb_migrate.py',
    )

    etl_script >> dbt_run >> dbt_quality_check >> duckdb_migration