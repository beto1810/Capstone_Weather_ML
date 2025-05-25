import requests
import json
import time
import os 
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer

load_dotenv()
    
def fetch_data_from_api():
    API_KEY = os.getenv('OPENWEATHER_API_KEY')
    if not API_KEY:
        raise ValueError("API key not found. Please set the OPENWEATHER_API_KEY environment variable.")
    city = "Thanh pho Ho Chi Minh"
    weather_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(weather_url)
    response.raise_for_status()
    return response.json()  # Expecting a list of events

def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Starting producer...", flush=True)
    try:
        while True:
            data = fetch_data_from_api()
            print("Fetched data from API", flush=True)
            data_weather = {
                'city': data['name'],
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'description': data['weather'][0]['description']
            }

            print("Sending data to Kafka...", flush=True)
            print(f"Data to send: {data_weather}", flush=True)
        
            producer.send('data-weather', data_weather).get(timeout=3)
            print("Data sent to Kafka", flush=True)

            print(f"Produced: {data_weather}", flush=True)

            time.sleep(5)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    produce_messages()

