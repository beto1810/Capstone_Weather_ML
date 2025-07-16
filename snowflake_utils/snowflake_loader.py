import os
from dotenv import load_dotenv
from snowflake.connector import connect
import logging

load_dotenv()

class SnowflakeConnectionSingleton:
    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SnowflakeConnectionSingleton, cls).__new__(cls)
            cls._connection = connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role = os.getenv("SNOWFLAKE_ROLE")
            )
        return cls._instance

    def get_connection(self):
        return self._connection
