import configparser
import psycopg2
from sql_queries import create_table_queries_list, drop_table_queries_list

# Iterates through the `drop_table_queries_list` and drops each table.
def drop_tables(cursor, connection):
    for query in drop_table_queries_list:
        cursor.execute(query)
        connection.commit()

# Iterates through the `create_table_queries_list` and create each table.
def create_tables(cursor, connection):
    for query in create_table_queries_list:
        cursor.execute(query)
        connection.commit()

# Create and drop tables main function - Sparkify DB
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    connection = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cursor = connection.cursor()

    drop_tables(cursor, connection)
    create_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
