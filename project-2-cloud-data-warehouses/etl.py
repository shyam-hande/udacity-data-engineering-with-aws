import configparser
import psycopg2
from sql_queries import copy_table_queries_list, insert_table_queries_list

# S3 to staging tables on Redshift.
def load_staging_tables(cursor, connection):    
    for query in copy_table_queries_list:
        cursor.execute(query)
        connection.commit()

# staging tables to analytics tables on Redshift.
def insert_tables(cursor, connection):
    for query in insert_table_queries_list:
        cursor.execute(query)
        connection.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connection = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cursor = connection.cursor()
    
    load_staging_tables(cursor, connection)
    insert_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
