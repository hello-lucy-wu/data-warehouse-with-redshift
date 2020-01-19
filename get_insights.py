import configparser
import psycopg2
from sql_queries import get_insights_queries
from time import time


def get_insights(cur, conn):
    for query in get_insights_queries:
        
        print('executing: ', query)
        t0 = time()

        cur.execute(query)
        conn.commit()

        loadTime = time() - t0
        
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

        row = cur.fetchone()
        if row is not None:
            print(row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    get_insights(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()