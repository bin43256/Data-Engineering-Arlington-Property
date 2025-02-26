from datetime import datetime
import time
import configparser
import asyncio
import json
import io

## UDFs
from s3_storage import S3_upload, get_multiple_files
from api_ingestion import get_sales_history, get_dwellings_general, get_dwellings_interior, get_property_class, get_property
from transformation import transform_sales, transform_property_class, transform_sale_date, transform_dwellings_general, transform_dwellings_interior, transform_property
from postgres import insert_data, truncate_tables


config = configparser.ConfigParser()
config.read('config.cfg')

# this would call each api and collect the data into a dictionary
async def collect_dataset():
    dataset = {
        'sales': await get_sales_history(),
        'dwellings_general': await get_dwellings_general(),
        'dwellings_interior': await get_dwellings_interior(),
        'property_class': await get_property_class(),
        'property': await get_property()
    }
    return dataset

# this would transform the data and return a dictionary to be loaded into the destination tables
def collect_transform_dataset(working_files,prefix):
    sale_date_df = transform_sale_date()
    sales_df, sales_type_df = transform_sales(working_files[prefix+'/sales'],sale_date_df)
    property_class_df = transform_property_class(working_files[prefix+'/property_class'])
    transform_dataset = {
                    'dim_sale_date':sale_date_df,
                    'dim_sales_type':sales_type_df,
                    'dim_dwellings_general':transform_dwellings_general(working_files[prefix+'/dwellings_general']),
                    'dim_dwellings_interior':transform_dwellings_interior(working_files[prefix+'/dwellings_interior']),
                    'dim_property_class': property_class_df,
                    'dim_property':transform_property(working_files[prefix+'/property'],property_class_df),
                    'fact_sales':sales_df
                    }
    return transform_dataset

async def main():
    start_time = time.time()
    dataset_collection = await collect_dataset()
    process_date = datetime.today().strftime('%Y-%m-%d')

    # upload to landing zone
    for dataset in dataset_collection:
        lz_filename = f"Landing_Zone/{process_date}/{dataset}"
        json_data = json.dumps(dataset_collection[dataset])
        json_file = io.BytesIO(json_data.encode('utf-8'))
        S3_upload(config.get('BUCKET', 'BUCKET_NAME'), json_file, lz_filename)

    # upload to working zone
    for dataset in dataset_collection:
        wz_filename = f"Working_Zone/{process_date}/{dataset}"
        json_data = json.dumps(dataset_collection[dataset])
        json_file = io.BytesIO(json_data.encode('utf-8'))
        S3_upload(config.get('BUCKET', 'BUCKET_NAME'), json_file, wz_filename)

    # download the files from working zone and transform the data
    prefix = f'Working_Zone/{process_date}'
    working_files = get_multiple_files(bucket_name=config.get('BUCKET', 'BUCKET_NAME'),prefix=prefix)
    transformed_datasets = collect_transform_dataset(working_files,prefix)

    # remove the volumes
    truncate_tables()

    # insert the transformed data into the destination tables
    for dataframe in transformed_datasets:
        insert_data(dataframe, transformed_datasets[dataframe])
    
    end_time =time.time()
    print(f'finishing all the etl jobs in {(end_time-start_time)/60} minute(s), move on to the next task')

if __name__ == '__main__':
    asyncio.run(main())