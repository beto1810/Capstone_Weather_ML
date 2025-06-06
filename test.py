import snowflake.connector
import json

# Cấu hình
config = {
    "account": "ZFLRNBO-YT37108",
    "user": "DATNGUYEN1810",  # thay bằng username thực tế
    "database": "KAFKA_AIRFLOW_WEATHER",
    "role": "USER_DBT_ROLE",
    "private_key_file": "/Users/dat/Documents/Weather_ML/keys/rsa_key.p8",
    # "password": None,  # Đảm bảo dòng này bị xóa hoặc không có
}

# Debug: in ra config không có private key
debug_config = {k: v for k, v in config.items() if 'key' not in k.lower()}
print("Using Snowflake config:")
print(json.dumps(debug_config, indent=4))

# Kết nối thử
ctx = snowflake.connector.connect(**config)