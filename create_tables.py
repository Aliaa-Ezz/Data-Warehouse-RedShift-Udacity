import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
This function is used to loop on all the drop statements rom sql_queries.py for all the tables we create
"""

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
This function is used to loop on all the create statements from sql_queries.py
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
"""
in the main function, first we get credentials from dwh.cfg
then we connect to the cluster
then use the funstion that drops tables
then create our tables.
"""

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()