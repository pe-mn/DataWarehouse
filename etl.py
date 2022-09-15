import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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


    
# TO CONNECT TO THE DATABASE
# --------------------------
# import psycopg2
## Use try except blocks
# conn=psycopg2.connect("dbname=dwh host=dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws port=5439 user=dwhuser password=Passw0rd")
# cur = conn.cursor()
# conn.set_session(autocommit=True)
# cur.execute("")

# OR

# postgresql://dwhuser:Passw0rd@dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com:5439/dwh
# conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
# print(conn_string)
# %sql $conn_string


# host = endpoint   