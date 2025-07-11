# import logging
# import os

# from dotenv import load_dotenv
# from snowflake.connector import connect

# load_dotenv()


# def load_to_snowflake(rows):
#     """Load weather data into Snowflake."""
#     try:
#         conn = connect(
#             user=os.getenv("SNOWFLAKE_USER"),
#             password=os.getenv("SNOWFLAKE_PASSWORD"),
#             account=os.getenv("SNOWFLAKE_ACCOUNT"),
#             warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
#             database=os.getenv("SNOWFLAKE_DATABASE"),
#             schema=os.getenv("SNOWFLAKE_SCHEMA"),
#         )

#         cursor = conn.cursor()

#         # Create a more detailed table schema matching the Weather API response
#         cursor.execute(
#             """
#             CREATE TABLE IF NOT EXISTS raw_weather_data (
#                 province_name VARCHAR,
#                 last_updated TIMESTAMP_NTZ,
#                 temp_c FLOAT,
#                 temp_f FLOAT,
#                 is_day BOOLEAN,
#                 condition_text VARCHAR,
#                 condition_icon VARCHAR,
#                 condition_code INTEGER,
#                 wind_mph FLOAT,
#                 wind_kph FLOAT,
#                 wind_degree INTEGER,
#                 wind_dir VARCHAR,
#                 pressure_mb FLOAT,
#                 pressure_in FLOAT,
#                 precip_mm FLOAT,
#                 precip_in FLOAT,
#                 humidity INTEGER,
#                 cloud INTEGER,
#                 feelslike_c FLOAT,
#                 feelslike_f FLOAT,
#                 vis_km FLOAT,
#                 vis_miles FLOAT,
#                 uv FLOAT,
#                 gust_mph FLOAT,
#                 gust_kph FLOAT,
#                 loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
#             )
#         """
#         )

#         # Insert data with all available fields
#         insert_query = """
#             INSERT INTO raw_weather_data (
#                 province_name, last_updated, temp_c, temp_f, is_day,
#                 condition_text, condition_icon, condition_code,
#                 wind_mph, wind_kph, wind_degree, wind_dir,
#                 pressure_mb, pressure_in, precip_mm, precip_in,
#                 humidity, cloud, feelslike_c, feelslike_f,
#                 vis_km, vis_miles, uv, gust_mph, gust_kph
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                     %s, %s, %s, %s, %s)
#         """

#         cursor.executemany(insert_query, rows)
#         conn.commit()
#         logging.info(f"✅ Successfully loaded {len(rows)} rows into Snowflake")

#     except Exception as e:
#         logging.error(f"❌ Error loading to Snowflake: {e}")
#         raise
#     finally:
#         if "conn" in locals():
#             conn.close()
