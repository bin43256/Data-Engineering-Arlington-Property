'''
This module would request a total of 6 APIs from the Arlington County RealEstate APIs,
the official data portal can be found at https://data.arlingtonva.us/home and the 
API documentation can be found at https://docs.data.arlingtonva.us/resources/#api
'''

import aiohttp
import asyncio
import time
import configparser
from logger import setup_logger

config = configparser.ConfigParser()
config.read('src/config.cfg') 
logger = setup_logger('api_log','elt_logs/api.log')

async def fetch_api_data(session, url): # helper function to start the async request
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(f"HTTP Error {response.status}: {await response.text()}")
                logger.debug('program sleep for 10s')
                await asyncio.sleep(10)
                return []
    except aiohttp.ClientError as e:
        logger.error(f"Error: {e}")
            
async def get_api_data(url: str, skip = 0, top = 10000):
    ''' 
    request data from the Arlington County RealEstate APIs, to increase the speed of data transfer,
    the function would request 5 api calls at a time
    param url: API Endpoints
    param skip: pagination starting point
    param top: workload per request
    return: json response
    '''
    data = []
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        logger.debug(f'processing the {url.split(sep="/")[-1]} API...')
        while True:
            api = url + f"&$skip={skip}&$top={top}"
            tasks.append(fetch_api_data(session, api))
            logger.debug(f"current starting point:{skip}")
            skip += 10000
            if len(tasks) >= 5:
                results = await asyncio.gather(*tasks)
                for chunk in results:
                    if chunk:
                        data.extend(chunk)
                        if len(chunk) < top:
                            logger.debug('reach the end of records, request stopped')
                            end_time = time.time()
                            logger.debug(f'API data loaded in: {(end_time - start_time)/60} minutes')
                            logger.debug(f'Total number of records loaded:{len(data)}')
                            return data 
                if len(data) % 10000 == 0:
                    logger.debug(f'current records of data loaded:{len(data)}')
                tasks = []

async def get_sales_history():
    url = (f"{config.get('APIS', 'SALES_HISTORY')}?$select="
           "salesHistoryKey,"
           "propertyKey,"
           "realEstatePropertyCode,"
           "salesTypeCode,"
           "salesTypeDsc,"
           "saleAmt,"
           "saleDate&$filter=saleDate gt 1980-01-01")
    return await get_api_data(url)

async def get_dwellings_general():
    url = (f"{config.get('APIS', 'DWELLINGS_GENERAL')}?$select="
           "dwellingKey,"
           "realEstatePropertyCode,"
           "coolingTypeDsc,"
           "dwellingTypeDsc,"
           "heatingTypeDsc,"
           "dwellingYearBuiltDate,"
           "storiesQuantityDsc,"
           "fireplaceCnt")
    return await get_api_data(url)

async def get_dwellings_interior():
    url = (f"{config.get('APIS', 'DWELLINGS_INTERIOR')}?$select="
           "improvementInteriorKey,"
           "realEstatePropertyCode,"
           "baseAreaSquareFeetQty,"
           "bedroomCnt,"
           "finishedAreaSquareFeetQty,"
           "twoFixtureBathroomCnt,"
           "threeFixtureBathroomCnt,"
           "fourFixtureBathroomCnt,"
           "fiveFixtureBathroomCnt,"
           "floorNbr,"
           "floorKey")
    return await get_api_data(url)

async def get_property_class():
    url = (f"{config.get('APIS', 'PROPERTY_CLASS')}?$select="
           "propertyClassTypeKey,"
           "propertyClassTypeCode,"
           "propertyClassTypeDsc")
    return await get_api_data(url)

async def get_property():
    url = (f"{config.get('APIS', 'PROPERTY')}?$select="
           "propertyKey,"
           "realEstatePropertyCode,"
           "propertyClassTypeCode,"
           "propertyClassTypeDsc,"
           "legalDsc,"
           "lotSizeQty,"
           "propertyStreetNbrNameText,"
           "propertyUnitNbr,"
           "propertyCityName,"
           "propertyZipCode")
    return await get_api_data(url)