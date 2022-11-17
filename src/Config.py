import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_URL = os.environ.get("DB_URL","")
DB_NAME = os.environ.get("DB_NAME","")
DB_USER = os.environ.get("DB_USER","")
DB_PASS = os.environ.get("DB_PASS","")
DB_PORT = os.environ.get("DB_PORT","")

MYSQL_URL = os.environ.get("MYSQL_URL","")
MYSQL_DB = os.environ.get("MYSQL_DB","")
MYSQL_USER = os.environ.get("MYSQL_USER","")
MYSQL_PASS = os.environ.get("MYSQL_PASS","")


