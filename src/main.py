from datetime import datetime
import time
import configparser
import asyncio

## UDFs
from s3_storage import get_multiple_files
from api_ingestion import get_sales_history, get_dwellings_interior, get_property_class, get_property
from transformation import transform_sales, transform_property_class, transform_sale_date, transform_dwellings_interior, transform_property
from postgres import insert_data, truncate_tables


config = configparser.ConfigParser()
config.read('config.cfg')

#this would call each api and collect the data into key-value pair
async def collect_dataset():
    dataset = {
        'sales': await get_sales_history(),
        'dwellings_interior': await get_dwellings_interior(),
        'property_class': await get_property_class(),
        'property': await get_property()
    }
    return dataset

# similarly, this would store key-value pair for transformed data
def collect_transform_dataset(wz_files,prefix):
    sale_date_df = transform_sale_date()
    sales_df, sales_type_df = transform_sales(wz_files[prefix+'/sales'],sale_date_df)
    property_class_df = transform_property_class(wz_files[prefix+'/property_class'])
    transform_dataset = {
                    'dim_sale_date':sale_date_df,
                    'dim_sales_type':sales_type_df,
                    'dim_dwellings_interior':transform_dwellings_interior(wz_files[prefix+'/dwellings_interior']),
                    'dim_property_class': property_class_df,
                    'dim_property':transform_property(wz_files[prefix+'/property'],property_class_df),
                    'fact_sales':sales_df
                    }
    return transform_dataset

async def main():
    start_time = time.time()
    # dataset_collection = await collect_dataset()
    bucket = "arlington-property-sales-data-lakehouse"
    process_date = datetime.today().strftime('%Y-%m-%d')

    #upload to S3 landing zone
    # for dataset in dataset_collection:
    #     lz_filename = f"Landing_Zone/{process_date}/{dataset}"
    #     json_data = json.dumps(dataset_collection[dataset])
    #     print(type(json_data))
    #     S3_upload(bucket=bucket, file=json_data, filename=lz_filename)

    # upload to S3 working zone
    # for dataset in dataset_collection:
    #     wz_filename = f"Working_Zone/{process_date}/{dataset}"
    #     json_data = json.dumps(dataset_collection[dataset])
    #     json_file = io.BytesIO(json_data.encode('utf-8'))
    #     S3_upload(bucket=bucket, file=json_file,filename=wz_filename)

    # download all the files from working zone and transform the data
    prefix = f'Working_Zone/{process_date}'
    wz_files = get_multiple_files(bucket=bucket,prefix=prefix)
    transformed_datasets = collect_transform_dataset(wz_files,prefix)

    # remove the volumes
    truncate_tables()

    # insert the transformed data into the destination tables
    for dataframe in transformed_datasets:
        insert_data(dataframe, transformed_datasets[dataframe])
    
    end_time =time.time()
    print(f'finishing all the etl in {(end_time-start_time)/60} minute(s), move on to the next task')

if __name__ == '__main__':
    asyncio.run(main())