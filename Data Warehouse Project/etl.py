import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, delete_table_duplicate_record_queries


def load_staging_tables(cur, conn):
    """
    Connects to the redshift database and copies data from the s3 bucket to the two staging tables.
    'copy_table_queries' can be found in sql_queries.py.
    
    Parameters:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print('Data was successfully copied to staging table.')


def insert_tables(cur, conn):
    """
    Connects to the redshift database and inserts data into the final tables from the two staging tables.
    'insert_table_queries' can be found in sql_queries.py.
    
    Parameters:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('Insert was successful.')

def delete_duplicates(cur, conn):
    """
    Connects to the redshift database and deletes any duplicate records from the user and aritist tables.
    Only the latest records are used.
    'delete_table_duplicate_record_queries' can be found in sql_queries.py.
    
    Parameters:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    for query in delete_table_duplicate_record_queries:
        cur.execute(query)
        conn.commit()
        print('Duplicate records were successfully deleted.')

def main():
    """
    1. Reads the dwh.cfg file and establishes a connection to the redshift database.
    2. Executes the load_staging_tables, insert_tables and delete_duplicates functions.
    3. Closes the connection.
    
    Parameters:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    print('Starting etl process.')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected successfully.')
    cur = conn.cursor()
    print('Cursor set successfully.')
    
    print('Loading data to staging tables...')
    load_staging_tables(cur, conn)
    print('Inserting data into final tables...')
    insert_tables(cur, conn)
    print('Deleting duplicate records in artist and user tables...')
    delete_duplicates(cur, conn)

    conn.close()
    print('Connection closed.')

if __name__ == "__main__":
    main()