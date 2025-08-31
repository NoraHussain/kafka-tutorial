# weather_producer.py

import time
import json
from kafka import KafkaProducer
import os 
import sys 
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))) 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

import config
import utils

# إنشاء Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=config.BROKER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ["Sana'a", "Aden", "Marib", "Hodeidah"]

while True:
    for city in cities:
        weather = utils.get_weather(city, config.API_KEY)
        if weather:
            now = datetime.datetime.now()
            weather['time'] = now.strftime("%H:%M")      # Hour:Minute
            weather['date'] = now.strftime("%Y-%m-%d")   # Date YYYY-MM-DD

            producer.send(
                config.TOPIC_NAME,
                key=city.encode("utf-8"),
                value=weather
            )
            print(f"Sent: {weather}")
    time.sleep(60)
