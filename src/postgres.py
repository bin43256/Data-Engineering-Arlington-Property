import psycopg2

default_db = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

# set up table attributes and schemas
def create_tables() -> None:
    with psycopg2.connect(**default_db) as conn:
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
                        realEstatePropertyCode INT,
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
                        propertyStreetNbrNameText VARCHAR(255),
                        propertyUnitNbr VARCHAR(255),
                        propertyCityName VARCHAR(255),
                        propertyZipCode VARCHAR(255),
                        propertyClassTypeKey INT
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
        print("all the table have been created")

def insert_data(table_name: str, json_data: list) -> None:
    """
    Inserts JSON data into the target PostgreSQL table.
    """
    print(f'Inserting into table {table_name}')
    try:
        with psycopg2.connect(**default_db) as conn:
            with conn.cursor() as cur:
                columns = json_data[0].keys()
                values_template = ','.join(['%s'] * len(columns))
                insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({values_template})"
                values = [[record[column] for column in columns] for record in json_data]                
                batch_size = 1000
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    cur.executemany(insert_query, batch)
                    print(f"Inserted batch of {len(batch)} records")
                print(f'Data inserted into {table_name} successfully!')       
    except Exception as e:
        print(f'An error occurred while inserting data: {str(e)}')
        raise

def truncate_tables():
    try:
        with psycopg2.connect(**default_db) as conn:
            with conn.cursor() as curs:
                curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                tables = curs.fetchall()
                for table in tables:
                    table_name = table[0]
                    curs.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                    print(f'table {table_name} has been truncated')
    except Exception as e:
        print(f"Error: {e}")
    print('all the tables have been truncated, continue...')

def export_data_to_csv(table_name: str, file_name: str) -> None:
    try:
        with psycopg2.connect(**default_db) as conn:
            with conn.cursor() as cur:
                with open(file_name, 'w') as f:
                    cur.copy_expert(f"COPY (SELECT * FROM {table_name}) TO STDOUT WITH CSV HEADER", f)
                print(f"Data exported to {file_name} successfully!")
    except Exception as e:
        print(f"Error exporting data: {e}")
        raise
