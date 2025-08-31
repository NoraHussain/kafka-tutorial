from kafka import KafkaConsumer
import json
import os 
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))) 

import config

consumer = KafkaConsumer(
    config.TOPIC_NAME,
    bootstrap_servers=config.BROKER_ADDRESS,
    group_id="alert-group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Alert Consumer listening...")

for msg in consumer:
    data = msg.value
    if data["temperature"] > 35:
        print(f"🔥 High temperature alert in {data['city']}: {data['temperature']}°C")
    else:
        print(f"Normal temperature in {data['city']}: {data['temperature']}°C")
