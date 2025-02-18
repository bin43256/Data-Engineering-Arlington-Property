from datetime import datetime
import time
import configparser
import asyncio
import json
import io
from s3_storage import S3_upload
from transformation import collect_transform_dataset
from postgres import create_tables, insert_data,drop_tables
from api_ingestion import collect_dataset


config = configparser.ConfigParser()
config.read('src/config.cfg')

async def main():
    start_time = time.time()
    
    dataset_collection = await collect_dataset() # dict that stores unprocessed data 
    process_date = datetime.today().strftime('%Y-%m-%d')
    
    # for dataset in dataset_collection:
    #     filename = f"{process_date}/{dataset}"
    #     json_data = json.dumps(dataset_collection[dataset])
    #     json_file = io.BytesIO(json_data.encode('utf-8'))
    #     S3_upload(config.get('BUCKET', 'LANDING_ZONE'),json_file,filename)

    '''
    function that called multiple transform functions that take unprocessed data as input
    and organize processed data into a dict
    '''
    transformed_datasets = collect_transform_dataset(dataset_collection)

    drop_tables() # kill the volumns for testing
    create_tables()

    for dataset in transformed_datasets:
        filename = f"{process_date}/{dataset}"
        insert_data(dataset, transformed_datasets[dataset])
        print(f'{dataset} has been insert into the target table')
    end_time =time.time()
    print(f'finishing all the jobs in {(end_time-start_time)/60} minute(s), exit program')

if __name__ == '__main__':
    asyncio.run(main())