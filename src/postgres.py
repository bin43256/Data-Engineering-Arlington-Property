import psycopg2
import adbc_driver_postgresql.dbapi as pg_dbapi
import pandas as pd

default_db = {
    'host': 'postgres_database',
    'port': 5432,
    'dbname': 'postgres_db',
    'user': 'postgres',
    'password': 'secret'
}
conn_string = 'postgres://postgres:secret@postgres_database:5432/postgres_db'

def insert_data(table_name: str, data: pd.DataFrame) -> None:
    """
    Inserts dataframe object into the target PostgreSQL table.
    """
    print(f'Inserting into table {table_name}')
    try:
        with pg_dbapi.connect(conn_string) as conn:
            data.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f'Data inserted into {table_name} successfully!')
    except Exception as e:
        print(f'An error occurred while inserting data: {str(e)}')

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

