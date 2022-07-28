import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
the database was created prior to running the create_tables.py script
the database's name is *dev*
"""



def drop_tables(cur, conn):
    """
    - It creates a connection and a cursor to our dev database in Redshift
    - It loops through the drop_table_queries list and:
        - it checks if the table already exists
        - it drops each table in the list 
    """   
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - It creates a connection and a cursor to our dev database in Redshift
    - It loops through the create_table_queries list and:
        - it creates each table in the list
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - It initialize the config parser to read configuration values from dwh.cfg file
    - It runs the drop_tables function
    - It runs the create table function
    - It finally close the connection to our Redshift cluster 
    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except psycopg2.Error as er:
        print("Error: Passing dwh strings to connection in create_tables script")
        print(e)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()