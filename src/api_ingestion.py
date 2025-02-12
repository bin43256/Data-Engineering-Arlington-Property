import aiohttp
import requests
import asyncio
import time
import configparser
from pathlib import Path
from s3_storage import get_all_files
from logger import setup_logger

config = configparser.ConfigParser()
config.read('src/config.cfg') 

logger = setup_logger('api_log','api.log')

async def fetch_api_data(session, url): # helper function to start the async request
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(f"HTTP Error {response.status}: {await response.text()}")
                return []
    except aiohttp.ClientError as e:
        logger.error(f"Error: {e}")
        return []
            
async def get_api_data(url: str, incre_load = False, skip = 0, top = 100 , stop = 1000):
    ''' 
    request api data from the Arlington County Data catalog, incremental load would only 
    apply on the sales history API
    param url: API Endpoints
    param incre_load: indicates if we are parsing the sales history API
    param skip: pagination starting point
    param top: workload per request
    return: json data
    '''
    data = []
    start_time = time.time()

    '''
    " think of async as restraunt where you order food, you don't have to wait for the food to be ready,
    you can tell the server to order second food while waiting for the first food to serve "
    '''
    async with aiohttp.ClientSession() as session:
        if incre_load:
            lz_files = list(get_all_files(bucket=config.get('BUCKET','LANDING_ZONE')))
            logger.debug('Processing the sales history API...')
            if not lz_files:
                logger.debug('No existing files found in the bucket, start the intial load')
                tasks = []
                while True:
                    api = f"{url}?$skip={skip}&$top={top}"
                    tasks.append(fetch_api_data(session, api))
                    skip +=100
                    if len(tasks) >=5:
                        results = await asyncio.gather(*tasks)
                        for chunk in results: # results should be the a list of 5 json data
                            data.extend(chunk)
                            if len(chunk) < top:
                                end_time = time.time()
                                logger.debug('reach the end of records, request stopped')
                                logger.debug(f'API data loaded in: {(end_time - start_time)/60} seconds')
                                return data
                        if len(data) >= stop:
                            logger.debug('reach the stop point, request stopped')
                            end_time = time.time()
                            logger.debug('API data loaded in: ', (end_time - start_time)/60, ' seconds')
                            return data
                        tasks = []
                        logger.debug(f'Total number of records loaded:{len(data)}')
                        await asyncio.sleep(0.1)
            else: # if any existing files found in the bucket, use most recent folder date as refresh point
                logger.debug('Existing files found in the bucket, start the incremental load')
                folders = set()
                for obj in lz_files:
                    folder_name = obj.key.split('/')[0] 
                    folders.add(folder_name)
                last_load_date = max(folders)
                logger.debug(f'Searching the last load date...: {last_load_date}')
                api = config.get('APIS', 'SALES_HISTORY') + '?$filter=saleDate gt ' + last_load_date
                response = requests.get(api)
                data_chunk = response.json()
                data.extend(data_chunk)
                logger.debug(f'Total number of records loaded:{len(data)}')
                return data
        else: # for other single-time API loads
            tasks = []
            logger.debug(f'processing the {url.split(sep='/')[-1]} API...')
            logger.debug('Start the intial load')
            while True:
                api = f"{url}?$skip={skip}&$top={top}"
                tasks.append(fetch_api_data(session, api))
                if len(tasks) >=5:
                    results = await asyncio.gather(*tasks)
                    for chunk in results:
                        data.extend(chunk)
                        if len(chunk) < top:
                            logger.debug('reach the end of records, request stopped')
                            end_time = time.time()
                            logger.debug(f'API data loaded in: {(end_time - start_time)/60} seconds')
                            logger.debug(f'Total number of records loaded:{len(data)}')
                            return data 
                    if len(data) >= stop:
                        end_time = time.time()
                        logger.debug('reach the stop point, request stopped')
                        logger.debug(f'Total number of records loaded:{len(data)}')
                        logger.debug(f'API data loaded in: {(end_time - start_time)/60} seconds')
                        return data
                    tasks = [] 

async def collect_dataset() -> dict:
    ''' 
    return a dictionary of api names and their corresponding json data
    '''
    sales = await get_api_data(url=config.get('APIS', 'SALES_HISTORY'), incre_load=True)
    time.sleep(1)
    logger.debug('preparing to load next API...')
    dwellings_general = await get_api_data(url=config.get('APIS', 'DWELLINGS_GENERAL'))
    time.sleep(1)
    logger.debug('preparing to load next API...')
    dwellings_interior = await get_api_data(url=config.get('APIS', 'DWELLINGS_INTERIOR'))
    time.sleep(1)
    logger.debug('preparing to load next API...')
    outbuildings = await get_api_data(url=config.get('APIS', 'OUTBUILDINGS'))
    time.sleep(1)
    logger.debug('preparing to load next API...')
    property = await get_api_data(url=config.get('APIS', 'PROPERTY'))
    logger.debug('all APIs loaded successfully')

    dataset_collection = {'sales':sales,
                          'dwellings_general':dwellings_general,
                          'dwellings_interior':dwellings_interior,
                          'outbuildings':outbuildings,
                          'property':property}
    return dataset_collection