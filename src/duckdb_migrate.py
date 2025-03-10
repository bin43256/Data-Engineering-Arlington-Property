import psycopg2
from postgres import default_db
import pandas as pd
import duckdb as dd

pg_conn = psycopg2.connect(**default_db)
duck_conn = dd.connect("datawarehouse.duckdb")

with pg_conn.cursor() as cur:
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = [row[0] for row in cur.fetchall()] 

for table in tables:
    print(f"Migrating table: {table}")
    df = pd.read_sql(f'SELECT * FROM "{table}"', pg_conn) 
    duck_conn.register(table, df)
    duck_conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{table}"')








