import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
in this file, we are meant to run the staging tables and insering data in fact and dimension tables
"""

"""
This first function is used to load the staging tables with data from sql_queries.py
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
This table is used to insert data into our tables.
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
in the main function, we first get our credentials then connect to the cluster then do loading and inseting.
"""
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