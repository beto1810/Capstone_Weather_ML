#!/usr/bin/env python3
"""
Test script to verify Snowflake connection and basic functionality.
Run this script to test your Snowflake setup before using the Streamlit app.
"""

import os
from dotenv import load_dotenv
from snowflake.connector import connect
import pandas as pd

# Load environment variables
load_dotenv()

def test_snowflake_connection():
    """Test Snowflake connection and basic operations."""
    print("🔍 Testing Snowflake Connection...")

    # Check environment variables
    required_vars = [
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("Please set these in your .env file")
        return False

    try:
        # Test connection
        print("📡 Connecting to Snowflake...")
        conn = connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )

        cursor = conn.cursor()

        # Test basic query
        print("🔍 Testing basic query...")
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ Snowflake version: {version}")

        # List tables
        print("📋 Listing available tables...")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        if tables:
            print("✅ Available tables:")
            for table in tables:
                print(f"  - {table[1]}")
        else:
            print("⚠️  No tables found in the current schema")

        # Test sample query on weather data
        print("🌤️  Testing weather data query...")
        try:
            cursor.execute("SELECT COUNT(*) as total_records FROM raw_weather_data")
            count = cursor.fetchone()[0]
            print(f"✅ Weather data records: {count}")

            if count > 0:
                cursor.execute("SELECT * FROM raw_weather_data LIMIT 3")
                sample_data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(sample_data, columns=columns)
                print("📊 Sample data:")
                print(df.to_string())
        except Exception as e:
            print(f"⚠️  Could not query weather data: {e}")

        conn.close()
        print("✅ Snowflake connection test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_snowflake_connection()
    if success:
        print("\n🎉 Your Snowflake setup is ready! You can now run the Streamlit app.")
    else:
        print("\n🔧 Please fix the issues above before running the Streamlit app.")