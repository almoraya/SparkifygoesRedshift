import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """
    - It creates a connection and a cursor to our dev database in Redshift
    - It loops 2 times through the copy_table_queries list and:
        - it loads data from an external S3 bucket to be later inserted in a previously created table
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - It creates a connection and a cursor to our dev database in Redshift
    - It appends the data loaded using the copy command - Bulk insert
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - It initialize the config parser to read configuration values from dwh.cfg file
    - It runs the load_staging_tables function
    - It runs the inser_tables function
    - It finally close the connection to our Redshift cluster 
    """
    
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except psycopg2.Error as er:
        print("Error: Passing dwh strings to connection in etl script")
        print(e)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()