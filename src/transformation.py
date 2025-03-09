
'''
This module would use pandas dataframe to perform data cleaning and transformation,
adn partition the data into star schema tables that would be ready to load for next step.

During the transformation, the system would produce metadata that describes different 
qualities of the data, such as data types, missing values, and duplicates.

Data lineage and dependencies would also be captured and documented for future reference.

'''
import pandas as pd
import json
import logging

def transform_sale_date():
    sale_date = pd.read_csv('data/sale_date.csv', header=0)
    sale_date['saleDateKey'] = sale_date['saleDateKey'].str.replace(',', '').astype(int)
    sale_date['saleDate'] = pd.to_datetime(sale_date['saleDate'])
    logging.debug(f'Sale Date columns with data types: {sale_date.dtypes}')
    return sale_date


def transform_dwellings_interior(data):
    di_df = pd.DataFrame(data=data)
    di_df = di_df.fillna('Not Applicable')
    di_df = di_df.drop_duplicates(subset=["improvementInteriorKey"])
    di_df['realEstatePropertyCode'] = di_df['realEstatePropertyCode'].astype(int)

    # attributes for change data capture
    di_df['expirationDate'] = pd.to_datetime('2200-12-31')
    di_df['effectiveDate'] = pd.Timestamp.today().date()
    # create surrogate key
    di_df = di_df.reset_index(drop=True)
    di_df['dwellingsInteriorSurKey'] = di_df.index

    logging.debug(f'Dwellings Interior DataFrame Info: {di_df.info()}')
    logging.debug(f'Dwellings Interior DataFrame Describe: {di_df.describe()}')
    logging.debug(f'Dwellings Interior columns with data types: {di_df.dtypes}')
    return di_df

def transform_property_class(data):
    pc_df = pd.DataFrame(data=data)
    logging.debug(f'Property Class columns with data types: {pc_df.dtypes}')
    return pc_df

def transform_property(data, property_class):
    df = pd.DataFrame(data=data)
    df.drop_duplicates(subset=["propertyKey"], inplace=True)
    df['realEstatePropertyCode'] = df['realEstatePropertyCode'].astype(int)
    df.drop(columns=["propertyClassTypeCode"], inplace=True)
    df.fillna('Not Applicable', inplace=True)
    property_df = df.merge(property_class, on="propertyClassTypeDsc", how="left")

    logging.debug(f'PROPERTY DataFrame Info: {property_df.info()}')
    logging.debug(f'PROPERTY DataFrame Describe: {property_df.describe()}')
    logging.debug(f'PROPERTY columns with data types: {property_df.dtypes}')
    return property_df

def transform_sales(data, sale_date_df):
    df = pd.DataFrame(data=data)
    df.dropna(axis=1, how='all', inplace=True)
    df.drop_duplicates(inplace=True, subset=["salesHistoryKey"])
    df["realEstatePropertyCode"] = df["realEstatePropertyCode"].astype(int)
    df['saleDate'] = pd.to_datetime(df['saleDate'])
    df.fillna('Not Applicable', inplace=True)

    #separate the sales type and merge the natural key
    sales_type_df = df[["salesTypeCode", "salesTypeDsc"]].drop_duplicates()
    sales_type_df['salesTypeKey'] = sales_type_df.reset_index().index + 1  # Simulating monotonically increasing ID
    df = df.merge(sales_type_df, on="salesTypeCode", how="left")
    #join the sale date
    df['saleDate'] =df['saleDate'].dt.tz_localize(None)
    df['saleDate'] = pd.to_datetime(df['saleDate']).dt.normalize()
    sale_date_df['saleDate'] = pd.to_datetime(sale_date_df['saleDate']).dt.normalize()
    df = df.merge(sale_date_df,on = 'saleDate', how = 'left')

    #create surrogate key and process date
    df['etlDate'] = pd.Timestamp.today().date()
    df['salesHistorySurKey'] = df.index

    sales_df = df[[
        "etlDate", 
        "salesHistorySurKey", 
        "salesHistoryKey", 
        "realEstatePropertyCode", 
        "propertyKey",
        "salesTypeKey", 
        "saleDateKey",
        "saleAmt"
    ]]

    logging.debug(f'SALES DataFrame Info: {sales_df.info()}')
    logging.debug(f'SALES DataFrame Describe: {sales_df.describe()}')
    logging.debug(f'SALES columns with data types: {sales_df.dtypes}')
    return sales_df, sales_type_df