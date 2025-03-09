'''
This module would request 4 APIs from the Arlington County RealEstate APIs,
the official data portal can be found at https://data.arlingtonva.us/home and the 
API documentation can be found at https://docs.data.arlingtonva.us/resources/#api
'''

import aiohttp
import asyncio
import time
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')

# API List
SALES_HISTORY = 'https://datahub-v2.arlingtonva.us/api/RealEstate/SalesHistory'
DWELLINGS_INTERIOR = 'https://datahub-v2.arlingtonva.us/api/RealEstate/ImprovementInterior'
PROPERTY = 'https://datahub-v2.arlingtonva.us/api/RealEstate/Property'
PROPERTY_CLASS = 'https://datahub-v2.arlingtonva.us/api/RealEstate/PropertyClassType'

# helper function for async request
async def fetch_api_data(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logging.error(f"HTTP Error {response.status}: {await response.text()}")
                logging.debug('program sleep for 10s')
                await asyncio.sleep(10)
                return []
    except aiohttp.ClientError as e:
        logging.error(f"Error: {e}")
            
async def get_api_data(query: str, skip = 0, top = 10000):
    ''' 
    request data from the Arlington County RealEstate APIs, to increase the speed of data transfer,
    the function would have 5 api calls at the time
    param url: API Endpoints
    param skip: pagination starting point
    param top: payload per request
    return: json response
    '''
    data = []
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        url = query.split(sep="?")[0]
        api_name = url.split(sep="/")[-1]
        logging.debug(f'\n{"-" * 50}\nprocessing the {api_name} API...')
        while True:
            api = query + f"&$skip={skip}&$top={top}"
            tasks.append(fetch_api_data(session, api))
            logging.debug(f"current starting point:{skip}")
            skip += 10000
            if len(tasks) >= 5:
                results = await asyncio.gather(*tasks)
                for chunk in results:
                    if chunk:
                        data.extend(chunk)
                        if len(chunk) < top:
                            logging.debug('reach the end of records, request stopped')
                            end_time = time.time()
                            logging.debug(f'API data loaded in: {(end_time - start_time)/60} minutes')
                            logging.debug(f'Total number of records loaded:{len(data)}')
                            return data 
                if len(data) % 10000 == 0:
                    logging.debug(f'current records of data loaded:{len(data)}')
                tasks = []

# calling each api and get the data now
async def get_sales_history():
    query = (f"{SALES_HISTORY}?$select="
           "salesHistoryKey,"
           "propertyKey,"
           "realEstatePropertyCode,"
           "salesTypeCode,"
           "salesTypeDsc,"
           "saleAmt,"
           "saleDate&$filter=saleDate gt 1980-01-01")
    return await get_api_data(query)

async def get_dwellings_interior():
    query = (f"{DWELLINGS_INTERIOR}?$select="
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
    return await get_api_data(query)

async def get_property_class():
    query = (f"{PROPERTY_CLASS}?$select="
           "propertyClassTypeKey,"
           "propertyClassTypeCode,"
           "propertyClassTypeDsc")
    return await get_api_data(query)

async def get_property():
    query = (f"{PROPERTY}?$select="
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
    return await get_api_data(query)