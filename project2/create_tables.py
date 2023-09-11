import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    
    """INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2'
    Output:
    drop every existed table by using queries in drop_table_queries"""
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """INPUT:
    cur : cursor object from psycopg2 connection to redshift database
    conn : connection object from psycopg2'
    Output:
    create each table by using queries in create_table_queries"""
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to redshift and get cursor
    then drop every existed table and create all table we need
    finally close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()