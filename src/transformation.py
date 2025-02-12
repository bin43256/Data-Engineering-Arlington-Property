from api_ingestion import collect_dataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import pandas as pd
from logger import setup_logger

logger = setup_logger('transform_log','logs/transformation.log')

def create_sparksession():
    logger.debug('running a spark session, starting the transformation')
    return SparkSession.builder \
        .appName("arlingtonPropertySale") \
        .master("local[*]") \
        .enableHiveSupport() \
        .config("spark.jars", "postgresql-42.7.5.jar") \
        .getOrCreate()
spark = create_sparksession()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def transform_sales_data(data):
    df = pd.DataFrame(data=data)
    logger.debug(f'null columns to be dropped: {df.columns[df.isnull().all()].tolist()}')

    df.dropna(axis=1,how='all',inplace=True)
    
    df = spark.createDataFrame(df)
    logger.debug(f'sales column data types: {df.dtypes}')
    df = df.withColumn("realEstatePropertyCode", col("realEstatePropertyCode").cast("int"))
    df = df.withColumn("salesTypeCode", col("salesTypeCode").cast("int"))
    df = df.withColumn("saleDate", to_date(col("saleDate"), "yyyy-MM-dd"))
    logger.debug(f'sales column data types: {df.dtypes}')
    df_select = df.select(
    "salesHistoryKey",
    "realEstatePropertyCode",
    "propertyStreetNbrNameText",
    "salesTypeCode",
    "salesTypeDsc",
    "saleAmt",
    "saleDate"
    )
    return df_select

def transform_dwellings_general(data):
    df = pd.DataFrame(data=data)
    df.dropna(axis=1,how='all',inplace=True)
    df = df.drop_duplicates()
    df = spark.createDataFrame(df)
    df = df.withColumn("realEstatePropertyCode", col("realEstatePropertyCode").cast("int"))
    df = df.withColumn("basementFinishedRecRoomSquareFeetQty", col("basementFinishedRecRoomSquareFeetQty").cast("int"))
    df_select = df.select(
    "dwellingKey",
    "propertyKey",
    "realEstatePropertyCode",
    "basementFinishedRecRoomSquareFeetQty",
    "basementRecRoomTypeDsc",
    "coolingTypeDsc",
    "dwellingTypeDsc",
    "heatingTypeDsc",
    "dwellingYearBuiltDate",
    "storiesQuantityDsc",
    "fireplaceCnt"
    )
    logger.debug(f'dwellings general column data types: {df_select.dtypes}')
    return df_select
def transform_dwellings_interior(data):
    df = pd.DataFrame(data=data)
    df.dropna(axis=1,how='all',inplace=True)
    df = df.drop_duplicates()
    df['bathroomCnt'] = df['twoFixtureBathroomCnt'] + df['threeFixtureBathroomCnt'] + df['fourFixtureBathroomCnt'] + df['fiveFixtureBathroomCnt']
    df = spark.createDataFrame(df)
    logger.debug(f'dwellings interior column data types: {df.dtypes}')
    df = df.withColumn("realEstatePropertyCode", col("realEstatePropertyCode").cast("int"))
    df_select = df.select(
    "improvementInteriorKey",
    "realEstatePropertyCode",
    "baseAreaSquareFeetQty",
    "bedroomCnt",
    "finishedAreaSquareFeetQty",
    "floorNbr",
    "floorKey",
    "bathroomCnt"
    )
    return df_select
def transform_outbuildings(data):
    df = pd.DataFrame(data=data)
    df.dropna(axis=1,how='all',inplace=True)
    df = df.drop_duplicates()
    df = spark.createDataFrame(df)
    df = df.withColumn("realEstatePropertyCode", col("realEstatePropertyCode").cast("int"))
    df_select = df.select(
    "outbuildingKey",
    "outbuildingBaseKey",
    "realEstatePropertyCode",
    "outbuildingTypeDsc",
    "outbuildingSquareFeetQty"
    )
    logger.debug(f'outbuildings column data types: {df.dtypes}')
    return df_select

def transform_property(data):
    df = pd.DataFrame(data=data)
    df.dropna(axis=1,how='all',inplace=True)
    df = df.drop_duplicates()
    df = spark.createDataFrame(df)
    logger.debug(f'property column data types: {df.dtypes}')
    df = df.withColumn("realEstatePropertyCode", col("realEstatePropertyCode").cast("int"))
    df = df.withColumn("propertyClassTypeCode", col("propertyClassTypeCode").cast("int"))
    df_select = df.select(
    "propertyKey",
    "realEstatePropertyCode",
    "propertyClassTypeCode",
    "propertyClassTypeDsc",
    "legalDsc",
    "lotSizeQty",
    "propertyStreetName",
    "propertyCityName",
    "propertyZipCode"
    )
    logger.debug(f'property column data types: {df_select.dtypes}')
    return df_select
def transform_data_collection(dataset_coll)-> dict:

    transformed_sale = transform_sales_data(dataset_coll['sales'])
    transformed_dwellings_general = transform_dwellings_general(dataset_coll['dwellings_general'])
    transformed_dwellings_interior = transform_dwellings_interior(dataset_coll['dwellings_interior'])
    transformed_outbuildings = transform_outbuildings(dataset_coll['outbuildings'])
    transformed_property = transform_property(dataset_coll['property'])

    transformed_dataset_coll = {'sales':transformed_sale,
                                'dwellings_general':transformed_dwellings_general,
                                'dwellings_interior':transformed_dwellings_interior,
                                'outbuildings':transformed_outbuildings,
                                'property':transformed_property}
    return transformed_dataset_coll