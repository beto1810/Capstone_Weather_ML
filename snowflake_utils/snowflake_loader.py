"""Singleton for managing Snowflake database connections."""
import os
import logging
from dotenv import load_dotenv
from snowflake.connector import connect

load_dotenv()

class SnowflakeConnectionSingleton:
    """Singleton class to manage a single Snowflake connection instance."""
    _instance = None
    _connection = None

    def __new__(cls):
        """Create or return the singleton instance, initializing the Snowflake connection if needed."""
        if cls._instance is None:
            cls._instance = super(SnowflakeConnectionSingleton, cls).__new__(cls)
            cls._connection = connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
        return cls._instance

    def get_connection(self):
        """Return the Snowflake connection object."""
        return self._connection
