import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """
    Description: This function is used to load data from s3 to staging tables in redshift cluster.
    Arguments:
        cur: a cursor object. 
        conn: connection to redshift cluster.
    Returns:
        None
    """
    for query in copy_table_queries:

        print('executing: ', query)
        t0 = time()

        cur.execute(query)
        conn.commit()

        loadTime = time() - t0
        
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def insert_tables(cur, conn):
    """
    Description: This function is used to insert data into tables in redshift cluster.
    Arguments:
        cur: a cursor object. 
        conn: connection to redshift cluster.
    Returns:
        None
    """
    for query in insert_table_queries:

        print('executing: ', query)
        t0 = time()

        cur.execute(query)
        conn.commit()

        loadTime = time() - t0
        
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()