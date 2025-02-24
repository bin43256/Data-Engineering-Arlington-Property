
'''
This module would use pandas dataframe to perform data cleaning and transformation,
adn partition the data into star schema tables that would be ready to load for next step.

During the transformation, the system would produce metadata that describes different 
qualities of the data, such as data types, missing values, and duplicates.

Data lineage and dependencies would also be captured and documented for future reference.

'''

import pandas as pd
import json
import io
from logger import setup_logger

# Set up our logger
logger = setup_logger('transform_log', 'elt_logs/transformation.log')

def transform_sale_date():
    sale_date = pd.read_csv('data/sale_date.csv', header=0)
    sale_date['saleDateKey'] = sale_date['saleDateKey'].str.replace(',', '').astype(int)
    sale_date['saleDate'] = pd.to_datetime(sale_date['saleDate'])
    logger.debug(f'SALE DATE columns with data types: {sale_date.dtypes}')
    return sale_date, sale_date.to_dict('records')

def transform_dwellings_general(data: io.BytesIO):
    # Convert BytesIO to JSON
    json_data = json.loads(data.read().decode('utf-8'))
    df = pd.DataFrame(data=json_data)
    df.dropna(axis=1, how='all', inplace=True)
    drop_duplicate_df = df.drop_duplicates(subset=['dwellingKey'], keep=False)
    
    # Convert column data types to integer
    drop_duplicate_df['realEstatePropertyCode'] = drop_duplicate_df['realEstatePropertyCode'].astype(int)

    dwellings_general_df = drop_duplicate_df[[
        "dwellingKey", "realEstatePropertyCode","coolingTypeDsc", "dwellingTypeDsc", "heatingTypeDsc", 
        "dwellingYearBuiltDate", "storiesQuantityDsc", "fireplaceCnt"
    ]]

    logger.debug(f'DWELLINGS GENERAL columns with data types: {dwellings_general_df.dtypes}')
    # Convert DataFrame to JSON records
    return dwellings_general_df.to_dict('records')

def transform_dwellings_interior(data: io.BytesIO):
    json_data = json.loads(data.read().decode('utf-8'))
    df = pd.DataFrame(data=json_data)
    df_select = df[[
        "improvementInteriorKey", "realEstatePropertyCode", "baseAreaSquareFeetQty", "bedroomCnt",
        "finishedAreaSquareFeetQty", "twoFixtureBathroomCnt", "threeFixtureBathroomCnt", "fourFixtureBathroomCnt",
        "fiveFixtureBathroomCnt", "floorNbr", "floorKey"
    ]]
    filled_df = df_select.fillna('Not Applicable')

    dwellings_interior_df = filled_df.drop_duplicates(subset=["improvementInteriorKey"])
    dwellings_interior_df['realEstatePropertyCode'] = dwellings_interior_df['realEstatePropertyCode'].astype(int)

    logger.debug(f'DWELLINGS INTERIOR columns with data types: {dwellings_interior_df.dtypes}')
    return dwellings_interior_df.to_dict('records')

def transform_property_class(data: io.BytesIO):
    json_data = json.loads(data.read().decode('utf-8'))
    property_class_df = pd.DataFrame(data=json_data)
    property_class_df['propertyClassTypeKey'] = property_class_df.reset_index().index + 1  # Simulating row number
    logger.debug(f'PROPERTY CLASS columns with data types: {property_class_df.dtypes}')

    return property_class_df, property_class_df.to_dict('records')

def transform_outbuildings(data: io.BytesIO):
    json_data = json.loads(data.read().decode('utf-8'))
    df = pd.DataFrame(data=json_data)
    df.dropna(axis=1, how='all', inplace=True)

    df['realEstatePropertyCode'] = df['realEstatePropertyCode'].astype(int)

    outbuilding_df = df[[
        "outbuildingKey", "outbuildingBaseKey", "realEstatePropertyCode", "outbuildingTypeDsc", "outbuildingSquareFeetQty"
    ]]
    logger.debug(f'OUTBUILDINGS columns with data types: {outbuilding_df.dtypes}')
    return outbuilding_df.to_dict('records')

def transform_property(data: io.BytesIO, property_class: io.BytesIO):
    json_data = json.loads(data.read().decode('utf-8'))
    df = pd.DataFrame(data=json_data)
    df.drop_duplicates(inplace=True)
    df['realEstatePropertyCode'] = df['realEstatePropertyCode'].astype(int)

    property_df = df[[
        "propertyKey", "realEstatePropertyCode","propertyClassTypeCode",
        "legalDsc", "lotSizeQty","propertyStreetNbrNameText", "propertyUnitNbr",
        "propertyCityName", "propertyZipCode"
    ]]
    property_df = property_df.merge(property_class, on="propertyClassTypeCode", how="left")
    property_df.drop(columns=["propertyClassTypeCode","propertyClassTypeDsc"], inplace=True)
    logger.debug(f'PROPERTY columns with data types: {property_df.dtypes}')
    return property_df.to_dict('records')

def transform_sales(data: io.BytesIO, sale_date_df):
    json_data = json.loads(data.read().decode('utf-8'))
    df = pd.DataFrame(data=json_data)
    df.dropna(axis=1, how='all', inplace=True)
    df.drop_duplicates(inplace=True, subset=["salesHistoryKey"])
    df["realEstatePropertyCode"] = df["realEstatePropertyCode"].astype(int)

    # Convert 'saleDate' to datetime
    df['saleDate'] = pd.to_datetime(df['saleDate'])
    df.fillna('Not Applicable', inplace=True)

    sales_type_df = df[["salesTypeCode", "salesTypeDsc"]].drop_duplicates()
    sales_type_df['salesTypeKey'] = sales_type_df.reset_index().index + 1  # Simulating monotonically increasing ID

    df = df.merge(sales_type_df[['salesTypeCode', 'salesTypeKey']], on="salesTypeCode", how="left")
    df['saleDate'] =df['saleDate'].dt.tz_localize(None)
    # Normalize both columns to remove the time part
    df['saleDate'] = pd.to_datetime(df['saleDate']).dt.normalize()
    sale_date_df['saleDate'] = pd.to_datetime(sale_date_df['saleDate']).dt.normalize()
    df = df.merge(sale_date_df,on = 'saleDate', how = 'left')
    sales_df = df[[
        "salesHistoryKey", "realEstatePropertyCode", "propertyKey", "salesTypeKey", "saleDateKey", "saleAmt"
    ]]
    
    print(f'\nSALES columns with data types: {sales_df.dtypes}')
    return sales_df.to_dict('records'), sales_type_df.to_dict('records')