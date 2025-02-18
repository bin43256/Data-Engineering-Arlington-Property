import psycopg2
import logging
from transformation import logger

default_db = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

def create_tables() -> None:
    with psycopg2.connect(
        host=default_db['host'],
        port=default_db['port'],
        dbname=default_db['dbname'],
        user=default_db['user'],
        password=default_db['password']
    ) as conn:
        print('postgres database connected sucessfully')
        with conn.cursor() as curs:
            curs.execute(
                """
                CREATE TABLE IF NOT EXISTS dim_sale_date
                    (
                        saleDateKey INT PRIMARY KEY,
                        saleDate DATE,
                        year INT,
                        Month INT,
                        day INT,
                        weekday INT,
                        quarter INT
                    );
                CREATE TABLE IF NOT EXISTS dim_sales_type
                    (
                        salesTypeKey INT PRIMARY KEY,
                        salesTypeCode VARCHAR(255),
                        salesTypeDsc VARCHAR(255)
                    );
                
                CREATE TABLE IF NOT EXISTS dim_dwellings_general
                (
                        dwellingKey INT PRIMARY KEY,
                        propertyKey INT,
                        realEstatePropertyCode INT,
                        basementFinishedRecRoomSquareFeetQty INT,
                        basementRecRoomTypeDsc VARCHAR(255),
                        coolingTypeDsc VARCHAR(255),
                        dwellingTypeDsc VARCHAR(255),
                        heatingTypeDsc VARCHAR(255),
                        dwellingYearBuiltDate INT,
                        storiesQuantityDsc VARCHAR(255),
                        fireplaceCnt INT
                    );

                CREATE TABLE IF NOT EXISTS dim_dwellings_interior
                    (
                        improvementInteriorKey INT PRIMARY KEY,
                        realEstatePropertyCode INT,
                        dwellingKey INT,
                        baseAreaSquareFeetQty INT,
                        bedroomCnt INT,
                        finishedAreaSquareFeetQty INT,
                        twoFixtureBathroomCnt INT,
                        threeFixtureBathroomCnt INT,
                        fourFixtureBathroomCnt INT,
                        fiveFixtureBathroomCnt INT,
                        floorNbr VARCHAR(255),
                        floorKey INT
                    );
                CREATE TABLE IF NOT EXISTS dim_property_class
                    (
                        propertyClassTypeKey INT PRIMARY KEY,
                        propertyClassTypeCode VARCHAR(255),
                        propertyClassTypeDsc VARCHAR(255)
                    );

                CREATE TABLE IF NOT EXISTS dim_outbuildings
                    (
                        outbuildingKey INT PRIMARY KEY,
                        outbuildingBaseKey INT,
                        realEstatePropertyCode INT,
                        outbuildingTypeDsc VARCHAR(255),
                        outbuildingSquareFeetQty INT
                    );

                CREATE TABLE IF NOT EXISTS dim_property
                    (
                        propertyKey INT PRIMARY KEY,
                        realEstatePropertyCode INT,
                        legalDsc VARCHAR(255),
                        lotSizeQty INT,
                        propertyStreetName VARCHAR(255),
                        propertStreetTypeCode VARCHAR(255),
                        propertyCityName VARCHAR(255),
                        propertyZipCode VARCHAR(255)
                    );
                CREATE TABLE IF NOT EXISTS fact_sales
                    (   
                        salesHistoryKey INT PRIMARY KEY,
                        realEstatePropertyCode INT,
                        propertyKey INT,
                        salesTypeKey INT,
                        saleDateKey INT,
                        FOREIGN KEY(salesTypeKey) REFERENCES dim_sales_type(salesTypeKey),
                        FOREIGN KEY(saleDateKey) REFERENCES dim_sale_Date(saleDateKey),
                        saleAmt DECIMAL
                    );
                """
            )

def insert_data(table_name: str, spark_df) -> None:
    #inserts a spark dataframe into the target database table
    print(f'inserting into table {table_name}')
    jdbc = f"jdbc:postgresql://{default_db['host']}:{default_db['port']}/{default_db['dbname']}"
    properties = {
    "user": default_db['user'],
    "password": default_db['password'],
    "driver": "org.postgresql.Driver"
    }
    try:
        spark_df.write \
        .jdbc(url=jdbc, table=table_name, mode="append", properties=properties)
    except Exception as e:
        print(f'A error has occured, check for connection, schema match, or duplication: {e}')

def drop_tables():
    try:
        with psycopg2.connect(
            host=default_db['host'],
            port=default_db['port'],
            dbname=default_db['dbname'],
            user=default_db['user'],
            password=default_db['password']
        ) as conn:
            with conn.cursor() as curs:
                curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                tables = curs.fetchall()
                print(f'all the tables: {tables}')
                for table in tables:
                    table_name = table[0]
                    curs.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                    print(f'table {table_name} has been dropped')
    except Exception as e:
        print(f"Error: {e}")
    print('all the tables have been dropped, continue...')