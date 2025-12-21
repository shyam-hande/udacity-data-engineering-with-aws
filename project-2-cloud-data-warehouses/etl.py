import configparser
import psycopg2
from sql_queries import copy_table_queries_list, insert_table_queries_list

"""
etl.py

Module to load data from S3 into Redshift staging tables and then insert data
from staging into the analytics tables.

This module reads Redshift cluster connection info from dwh.cfg and executes
the COPY and INSERT queries defined in sql_queries.py.
"""

def load_staging_tables(cursor, connection):

    """
    Load data from S3 into Redshift staging tables.

    Executes each COPY query from `copy_table_queries_list` to load data from S3
    into Redshift staging tables. The connection is committed after each query.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor for the database connection.
        connection (psycopg2.extensions.connection): Active database connection.

    Returns:
        None
    """
    
    for query in copy_table_queries_list:
        cursor.execute(query)
        connection.commit()


def insert_tables(cursor, connection):

    """
    Insert data from staging tables into analytics tables.

    Executes each INSERT query from `insert_table_queries_list` to populate the
    analytics/schema tables from the staging tables. The connection is committed
    after each query.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor for the database connection.
        connection (psycopg2.extensions.connection): Active database connection.

    Returns:
        None
    """
    
    for query in insert_table_queries_list:
        cursor.execute(query)
        connection.commit()


def main():

    """
    Read configuration, connect to the cluster, load and insert tables.

    Reads Redshift cluster connection parameters from 'dwh.cfg', opens a
    connection to the database, runs the staging loads and inserts, and closes
    the connection.

    Returns:
        None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connection = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cursor = connection.cursor()
    
    load_staging_tables(cursor, connection)
    insert_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
