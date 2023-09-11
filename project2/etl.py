import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,query_tables_queries


def load_staging_tables(cur, conn):
    """INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2'
    Output:
    copy data from s3 to redshift"""
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2'
    Output:
    insert data into the table we created before"""
    for query in insert_table_queries: 
        print(query)
        cur.execute(query)
        conn.commit()

def query_tables(cur,conn):
    """INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2'
    Output:
    get the answer for questions"""
    for query in query_tables_queries:
        print(query)
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()


def main():
    """
    Connect to redshift and get cursor
    then copy data from s3 to redshift
    and load into different tables
    finally close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    query_tables(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()