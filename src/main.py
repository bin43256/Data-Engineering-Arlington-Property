from datetime import datetime
import time
import configparser
import asyncio
import json
import io

## UDFs
from s3_storage import S3_upload, get_multiple_files
from api_ingestion import get_sales_history, get_dwellings_general, get_dwellings_interior, get_property_class, get_outbuildings, get_property
from transformation import transform_sales, transform_property_class, transform_sale_date, transform_dwellings_general, transform_dwellings_interior, transform_outbuildings, transform_property
from postgres import create_tables, insert_data, truncate_tables


config = configparser.ConfigParser()
config.read('src/config.cfg')

# this would call each api and collect the data into a dictionary
async def collect_dataset():
    dataset = {
        'sales': await get_sales_history(),
        'dwellings_general': await get_dwellings_general(),
        'dwellings_interior': await get_dwellings_interior(),
        'property_class': await get_property_class(),
        'outbuildings': await get_outbuildings(),
        'property': await get_property()
    }
    return dataset

# this would transform the data and return a dictionary to be loaded into the destination tables
def collect_transform_dataset(working_files):
    sale_date_df,sale_date_json = transform_sale_date()
    sales, sales_type = transform_sales(working_files['Working_Zone/2025-02-21/sales'],sale_date_df)
    property_class_df,property_class_json = transform_property_class(working_files['Working_Zone/2025-02-21/property_class'])
    transform_dataset = {
                    'dim_sale_date':sale_date_json,
                    'dim_sales_type':sales_type,
                    'dim_dwellings_general':transform_dwellings_general(working_files['Working_Zone/2025-02-21/dwellings_general']),
                    'dim_dwellings_interior':transform_dwellings_interior(working_files['Working_Zone/2025-02-21/dwellings_interior']),
                    'dim_property_class': property_class_json,
                    'dim_outbuildings':transform_outbuildings(working_files['Working_Zone/2025-02-21/outbuildings']),
                    'dim_property':transform_property(working_files['Working_Zone/2025-02-21/property'],property_class_df),
                    'fact_sales':sales
                    }
    return transform_dataset

async def main():
    start_time = time.time()
    dataset_collection = await collect_dataset()
    process_date = datetime.today().strftime('%Y-%m-%d')

    # compile raw data and upload to S3 bucket
    for dataset in dataset_collection:
        lz_filename = f"Landing_Zone/{process_date}/{dataset}"
        json_data = json.dumps(dataset_collection[dataset])
        json_file = io.BytesIO(json_data.encode('utf-8'))
        S3_upload(config.get('BUCKET', 'BUCKET_NAME'), json_file, lz_filename)
    for dataset in dataset_collection:
        wz_filename = f"Working_Zone/{process_date}/{dataset}"
        json_data = json.dumps(dataset_collection[dataset])
        json_file = io.BytesIO(json_data.encode('utf-8'))
        S3_upload(config.get('BUCKET', 'BUCKET_NAME'), json_file, wz_filename)

    # download the working files from s3 and transform the data
    working_files = get_multiple_files(config.get('BUCKET', 'BUCKET_NAME'),'Working_Zone/2025-02-21')
    transformed_datasets = collect_transform_dataset(working_files)

    truncate_tables() # remove the volumns during each load
    create_tables()

    # insert the transformed data into the destination tables
    for dataset in transformed_datasets:
        insert_data(dataset, transformed_datasets[dataset])
    
    end_time =time.time()
    print(f'finishing all the etl jobs in {(end_time-start_time)/60} minute(s), move on to the next task')

if __name__ == '__main__':
    asyncio.run(main())