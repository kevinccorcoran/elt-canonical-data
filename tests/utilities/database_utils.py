# File: database_utils.py

import os
from psycopg2 import pool
from dotenv import load_dotenv

# Load environment variables from a specific .env file
# Adjust the path as necessary for your project
load_dotenv('/Users/kevin/Dropbox/applications/ELT/.env.py')

class Database:
    __connection_pool = None

    @staticmethod
    def initialize():
        if Database.__connection_pool is None:
            Database.__connection_pool = pool.SimpleConnectionPool(
                1,
                10,
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )

    @staticmethod
    def get_connection():
        Database.initialize()  # Ensure the connection pool is initialized
        return Database.__connection_pool.getconn()

    @staticmethod
    def return_connection(connection):
        Database.__connection_pool.putconn(connection)

    @staticmethod
    def close_all_connections():
        Database.__connection_pool.closeall()
