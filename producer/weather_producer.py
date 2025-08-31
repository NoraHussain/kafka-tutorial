# weather_producer.py

import time
import json
from kafka import KafkaProducer
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

# Import config.py as a module
import config
import utils

producer = KafkaProducer(
    bootstrap_servers=config.BROKER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    weather = utils.get_weather(config.CITY, config.API_KEY)
    if weather:
        producer.send(config.TOPIC_NAME, weather)
        print(f"Sent: {weather}")
    time.sleep(10)
