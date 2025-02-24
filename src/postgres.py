import psycopg2

default_db = {
    'host': 'postgres_database',
    'port': 5434,
    'dbname': 'postgres_db',
    'user': 'postgres',
    'password': 'secret'
}

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

