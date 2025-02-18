from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField,IntegerType,DateType,StringType
import pandas as pd
from logger import setup_logger

# set up our logger and create a global spark session
logger = setup_logger('transform_log','logs/transformation.log')
def create_sparksession():
    return SparkSession.builder \
        .appName("arlingtonPropertySale") \
        .master("local[*]") \
        .enableHiveSupport() \
        .config("spark.jars", "postgresql-42.7.5.jar") \
        .getOrCreate()
spark = create_sparksession()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
logger.debug('running a spark session, starting the transformation')

def transform_sale_date():
    sale_date = spark.read.csv('data/sale_date.csv',header = True)
    sale_date = sale_date.withColumn("saleDate", F.to_date(F.col("saleDate"), "MM/dd/yyyy"))
    sale_date = sale_date.withColumn("saleDateKey", F.col("saleDateKey").cast("int"))
    sale_date = sale_date.withColumn("year", F.col("year").cast("int"))
    sale_date = sale_date.withColumn("month", F.col("month").cast("int"))
    sale_date = sale_date.withColumn("day", F.col("day").cast("int"))
    sale_date = sale_date.withColumn("weekday", F.col("weekday").cast("int"))
    sale_date = sale_date.withColumn("quarter", F.col("quarter").cast("int"))
    logger.debug(f'SALE DATE columns with data types: {sale_date.dtypes}')
    return sale_date

def transform_dwellings_general(data:dict):
    df = pd.DataFrame(data=data)
    logger.debug(f'The columns that are null will be dropped: {df.columns[df.isnull().all()].tolist()}')
    df.dropna(axis=1,how='all',inplace=True)
    drop_duplicate_df = df[~df['dwellingKey'].duplicated(keep=False)]  # 'keep=False' shows all occurrences of duplicates


    df = spark.createDataFrame(drop_duplicate_df)
    df = df.withColumn("realEstatePropertyCode", F.col("realEstatePropertyCode").cast("int"))
    df = df.withColumn("basementFinishedRecRoomSquareFeetQty", F.col("basementFinishedRecRoomSquareFeetQty").cast("int"))

    dwellings_general_df = df.select(
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

    #duplicates = dwellings_general_df.groupby(*dwellings_general_df.columns).count().filter(F.col('count')>1)
    logger.debug(f'DWELLINGS GENERAL columns with data types: {dwellings_general_df.dtypes}')
    return dwellings_general_df

def transform_dwellings_interior(data):
    df = pd.DataFrame(data=data)
    df_select = df[
    ["improvementInteriorKey",
    "realEstatePropertyCode",
    "baseAreaSquareFeetQty",
    "bedroomCnt",
    "finishedAreaSquareFeetQty",
    "twoFixtureBathroomCnt",
    "threeFixtureBathroomCnt",
    "fourFixtureBathroomCnt",
    "fiveFixtureBathroomCnt",
    "floorNbr",
    "floorKey",]
    ]
    filled_df = df_select.fillna('Not Applicable')

    dwellings_interior_df = spark.createDataFrame(filled_df)
    dwellings_interior_df = dwellings_interior_df.dropDuplicates(["improvementInteriorKey"])
    dwellings_interior_df = dwellings_interior_df.withColumn("realEstatePropertyCode", F.col("realEstatePropertyCode").cast("int"))

    logger.debug(f'DWELLINGS INTERIOR columns with data types: {dwellings_interior_df.dtypes}')
    return dwellings_interior_df

def transform_property_class(data):
    property_class_df = spark.createDataFrame(data)
    window = Window.orderBy("propertyClassTypeCode")
    property_class_df = property_class_df.withColumn( "propertyClassTypeKey", F.row_number().over(window))
    logger.debug(f'PROPERTY CLASS columns with data types: {property_class_df.dtypes}')
    return property_class_df

def transform_outbuildings(data):
    df = pd.DataFrame(data=data)
    logger.debug(f'The columns that are null will be dropped: {df.columns[df.isnull().all()].tolist()}')
    df.dropna(axis=1,how='all',inplace=True)

    df = spark.createDataFrame(df)
    df = df.withColumn("realEstatePropertyCode", F.col("realEstatePropertyCode").cast("int"))

    outbuiding_df = df.select(
    "outbuildingKey",
    "outbuildingBaseKey",
    "realEstatePropertyCode",
    "outbuildingTypeDsc",
    "outbuildingSquareFeetQty"
    )
    logger.debug(f'OUTBUILDINGS columns with data types: {outbuiding_df.dtypes}')
    return outbuiding_df

def transform_property(data):
    df = pd.DataFrame(data=data)
    df = df.drop_duplicates()
    df = spark.createDataFrame(df)
    df = df.withColumn("realEstatePropertyCode", F.col("realEstatePropertyCode").cast("int"))

    property_df = df.select(
    "propertyKey",
    "realEstatePropertyCode",
    "legalDsc",
    "lotSizeQty",
    "propertyStreetName",
    "propertyCityName",
    "propertyZipCode"
    )
    logger.debug(f'PROPERTY columns with data types: {property_df.dtypes}')
    return property_df

def transform_sales(data:dict):
    '''
    process the sales api data
    return:
        sales as our fact table
        sales_type as our dim table
    '''
    df = pd.DataFrame(data=data)
    df.dropna(axis=1,how='all',inplace=True)
    
    df = spark.createDataFrame(df)
    df = df.withColumn("realEstatePropertyCode", F.col("realEstatePropertyCode").cast("int"))
    df = df.withColumn('saleDate', F.col("saleDate").cast("date"))
    df = df.fillna('Not Applicable')
    sales_type_df = df.select(
        "salesTypeCode",
        "salesTypeDsc",
    )
    sales_type_df = sales_type_df.distinct().withColumn('salesTypeKey',F.monotonically_increasing_id())
    df = df.join(sales_type_df, on = "salesTypeCode")

    sales_date_df = transform_sale_date()
    df = df.join(sales_date_df, on = 'saleDate')

    sales_df = df.select(
    "salesHistoryKey",
    "realEstatePropertyCode",
    "propertyKey",
    "salesTypeKey",
    "saleDateKey",
    "saleAmt",
    )
    sales_df = sales_df.dropDuplicates()
    logger.debug(f"number of rows remain:{sales_df.count()}")
    logger.debug(f'SALES columns with data types: {sales_df.dtypes}')
    return sales_df, sales_type_df

def collect_transform_dataset(dataset_coll)-> dict:
    sales, sales_type = transform_sales(dataset_coll['sales'])
    dataset_coll = {
                    'dim_sale_date':transform_sale_date(),
                    'dim_sales_type':sales_type,
                    'dim_dwellings_general':transform_dwellings_general(dataset_coll['dwellings_general']),
                    'dim_dwellings_interior':transform_dwellings_interior(dataset_coll['dwellings_interior']),
                    'dim_property_class': transform_property_class(dataset_coll['property_class']),
                    'dim_outbuildings':transform_outbuildings(dataset_coll['outbuildings']),
                    'dim_property':transform_property(dataset_coll['property']),
                    'fact_sales':sales
                    }
    return dataset_coll