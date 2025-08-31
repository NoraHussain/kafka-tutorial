# weather_consumer.py

from kafka import KafkaConsumer
import json


import os 
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))) 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

import config
import utils

consumer = KafkaConsumer(
    config.TOPIC_NAME,
    bootstrap_servers=config.BROKER_ADDRESS,
    group_id="weather-group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(
        f"City: {message.value['city']} | "
        f"Temp: {message.value['temperature']}°C | "
        f"Partition: {message.partition} | "
        f"Offset: {message.offset}"
    )

