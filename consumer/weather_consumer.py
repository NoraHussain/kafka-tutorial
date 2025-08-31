# weather_consumer.py

import json
from kafka import KafkaConsumer
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

import config
import utils

consumer = KafkaConsumer(
    config.TOPIC_NAME,
    bootstrap_servers=config.BROKER_ADDRESS,
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for msg in consumer:
    data = msg.value
    print(f"City: {data['city']} | Temp: {data['temperature']}°C | Humidity: {data['humidity']}% | Condition: {data['description']}")
