import os
import sys
sys.path.append(os.getcwd())

import asyncpg
import asyncio

from src import Config
#pip install --upgrade google-cloud-bigquery
from google.cloud import bigquery 
import mysql.connector
# Credential Postgres
SERVER_DB = Config.DB_URL
DATABASE_DB = Config.DB_NAME
USERNAME_DB = Config.DB_USER
PASSWORD_DB = Config.DB_PASS
PORT_DB = Config.DB_PORT

#credential mysql
MYSQL_SERVER_DB = Config.MYSQL_URL
MYSQL_DATABASE_DB = Config.MYSQL_DB
MYSQL_USERNAME_DB = Config.MYSQL_USER
MYSQL_PASSWORD_DB = Config.MYSQL_PASS


class DBClient:
    ####### setup connection postgres#######
    async def ConnectionDB():
        conn = None
        if conn == None:
            try:
                conn = await asyncpg.connect(
                        user=USERNAME_DB,
                        password=PASSWORD_DB,
                        database=DATABASE_DB,
                        host=SERVER_DB,
                        port=PORT_DB,
                    )
                print(f"[INFO]success connect to postgresDB...")
                # await conn.close()
            except asyncpg.PostgresError as exc:
                return ("Failed to initialise database.", exc)
            else:
                pass
        return conn
    
    ######### setup connection bigquery ##########
    async def ConnectionBQ():
        conn = None
        if conn == None:
            try:
                conn = os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
                '/home/tananugrahr/learning-data-engineering-1-d66611d5f02e.json'
                client = bigquery.Client.from_service_account_json(conn)
                print(f"[INFO]success connect to Big Query...")
            except:
                print("error")
        return conn
    ##### setup connection db mysql #########
    #https://pynative.com/python-mysql-select-query-to-fetch-data/
    async def ConnectionMysql():
        conn = None
        if conn == None:
            try:
                conn = mysql.connector.connect(host = MYSQL_SERVER_DB,
                                                database = MYSQL_DATABASE_DB,
                                                user = MYSQL_USERNAME_DB,
                                                password = MYSQL_PASSWORD_DB
                                         )
                print(f"[INFO]success connect to Mysql DB...")
            except mysql.connector.Error as error:
                print("Error while connecting to MySQL", error)
            else:
                pass
        return conn
            
               