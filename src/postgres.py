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
                CREATE TABLE IF NOT EXISTS sales
                    (   
                        salesHistoryKey INT PRIMARY KEY,
                        realEstatePropertyCode INT,
                        propertyStreetNbrNameText VARCHAR(255),
                        salesTypeCode INT,
                        salesTypeDsc VARCHAR(255),
                        saleAmt DECIMAL,
                        saleDate DATE
                    );
                CREATE TABLE IF NOT EXISTS dwellings_general
                (
                dwellingKey INT,
                propertyKey INT,
                realEstatePropertyCode INT,
                basementFinishedRecRoomSquareFeetQty INT,
                basementRecRoomTypeDsc VARCHAR(255),
                coolingTypeDsc VARCHAR(255),
                dwellingTypeDsc VARCHAR(255),
                heatingTypeDsc VARCHAR(255),
                dwellingYearBuiltDate INT,
                storiesQuantityDsc VARCHAR(255),
                fireplaceCnt INT,
                PRIMARY KEY (dwellingKey, propertyKey,realEstatePropertyCode)
                );

                CREATE TABLE IF NOT EXISTS dwellings_interior
                    (
                        improvementInteriorKey INT PRIMARY KEY,
                        realEstatePropertyCode INT,
                        baseAreaSquareFeetQty INT,
                        bedroomCnt INT,
                        finishedAreaSquareFeetQty INT,
                        floorNbr VARCHAR(255),
                        floorKey INT,
                        bathroomcnt INT
                    );

                CREATE TABLE IF NOT EXISTS outbuildings
                    (
                        outbuildingKey INT PRIMARY KEY,
                        outbuildingBaseKey INT,
                        realEstatePropertyCode INT,
                        outbuildingTypeDsc VARCHAR(255),
                        outbuildingSquareFeetQty INT
                    );

                CREATE TABLE IF NOT EXISTS property
                    (
                        propertyKey INT PRIMARY KEY,
                        realEstatePropertyCode INT,
                        propertyClassTypeCode INT,
                        propertyClassTypeDsc VARCHAR(255),
                        legalDsc VARCHAR(255),
                        lotSizeQty INT,
                        propertyStreetName VARCHAR(255),
                        propertStreetTypeCode VARCHAR(255),
                        propertyCityName VARCHAR(255),
                        propertyZipCode VARCHAR(255)
                    );
                """
            )

def insert_data(table_name: str, spark_df) -> None:
    ''' insert data- maybe a saprk dataframe would work? into the table
    param table_name: name of the table
    param data: data to be inserted
    '''    
    jdbc = f"jdbc:postgresql://{default_db['host']}:{default_db['port']}/{default_db['dbname']}"
    properties = {
    "user": default_db['user'],
    "password": default_db['password'],
    "driver": "org.postgresql.Driver"
    }
    try:
        spark_df.write \
        .jdbc(url=jdbc, table=table_name, mode="append", properties=properties)
        logger.debug(f'inserting data into {table_name}')
    except KeyError as e:
        print(f'A key error has occured: {e}')

def clear_tables_with_data():
    try:
        # Connect to PostgreSQL database
        with psycopg2.connect(
            host=default_db['host'],
            port=default_db['port'],
            dbname=default_db['dbname'],
            user=default_db['user'],
            password=default_db['password']
        ) as conn:
            print("Postgres database connected successfully")
            with conn.cursor() as curs:
                curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                tables = curs.fetchall()
                for table in tables:
                    table_name = table[0]

                    curs.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = curs.fetchone()[0]
                    if row_count > 0:
                        # Drop the table if it has data
                        curs.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                        print(f"Table {table_name} has data and has been dropped.")
                    else:
                        print(f"Table {table_name} has no data, skipping.")
                conn.commit()

    except Exception as e:
        print(f"Error: {e}")
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
                for table in tables:
                    table_name = table[0]
                    curs.execute(f"DROP TABLE IF EXISTS {table_name};")
    except Exception as e:
        print(f"Error: {e}")
    print('all the tables have been dropped, continue...')
