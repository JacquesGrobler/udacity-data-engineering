import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Connects to the redshift database and drops all the tables if they exist.
    'drop_table_queries' can be found in sql_queries.py.
    
    Parameters:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Table dropped successfully.')


def create_tables(cur, conn):
    """
    Connects to the redshift database and creates all the necessary tables.
    'create_table_queries' can be found in sql_queries.py.
    
    Parameters:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('Table created successfully.')


def main():
    """
    1. Reads the dwh.cfg file and establishes a connection to the redshift database.
    2. Executes the drop_tables and create_tables functions.
    3. Closes the connection.
    """
    print('Starting table drop and creation process.')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected successfully.')
    cur = conn.cursor()
    print('Cursor set successfully.')

    print('Dropping tables...')
    drop_tables(cur, conn)
    print('Creating tables...')
    create_tables(cur, conn)

    conn.close()
    print('Connection closed.')

if __name__ == "__main__":
    main()