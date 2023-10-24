import snowflake.connector

USER = 'pgadmin'
PASSWORD = 'Caef--1557'
WAREHOUSE = 'COMPUTE_WH'
ACCOUNT = 'nmkwolx-nk95043'
DATABASE = 'SPOTIFY_DATA'
SCHEMA = 'RAW'
ROLE = 'ACCOUNTADMIN'


def spotify_raw_conn():
    conn = snowflake.connector.connect(
            user=USER
            ,password=PASSWORD
            ,account=ACCOUNT
            ,warehouse=WAREHOUSE
            ,database=DATABASE
            ,schema=SCHEMA
            ,role=ROLE)
    return conn

if __name__ == '__main__':
    conx = spotify_raw_conn()


