import configparser
import psycopg2
from sql_queries import create_table_queries_list, drop_table_queries_list

"""
create_tables.py

Module to drop and create tables in the Sparkify Redshift cluster.

This module reads cluster connection info from dwh.cfg and uses queries defined
in sql_queries.py (create_table_queries_list, drop_table_queries_list) to drop
and create tables.

"""
def drop_tables(cursor, connection):

    """
    Drop tables listed in `drop_table_queries_list`.

    Iterates through the `drop_table_queries_list` and executes each SQL
    statement using the provided cursor. The connection is committed after
    each statement to persist changes.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor for the database connection.
        connection (psycopg2.extensions.connection): Active database connection.

    """
    
    for query in drop_table_queries_list:
        cursor.execute(query)
        connection.commit()


def create_tables(cursor, connection):

    """
    Create tables listed in `create_table_queries_list`.

    Iterates through the `create_table_queries_list` and executes each SQL
    statement using the provided cursor. The connection is committed after
    each statement to persist changes.

    Args:
        cursor (psycopg2.extensions.cursor): Cursor for the database connection.
        connection (psycopg2.extensions.connection): Active database connection.

    """
    
    for query in create_table_queries_list:
        cursor.execute(query)
        connection.commit()


def main():

    """
    Read configuration, connect to the cluster, drop and create tables.
    Reads Redshift cluster connection parameters from 'dwh.cfg', opens a
    connection to the database, drops existing tables, creates tables, and
    closes the connection.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    connection = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cursor = connection.cursor()

    drop_tables(cursor, connection)
    create_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
