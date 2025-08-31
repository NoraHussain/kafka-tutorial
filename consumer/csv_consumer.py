from kafka import KafkaConsumer
import json
import csv
import os 
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))) 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

import config
import utils

consumer = KafkaConsumer(
    config.TOPIC_NAME,
    bootstrap_servers=config.BROKER_ADDRESS,
    group_id="csv-group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

output_file = "weather_data.csv"
file_exists = os.path.isfile(output_file)

with open(output_file, mode='a', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=["city", "temperature", "humidity", "description"])
    if not file_exists:
        writer.writeheader()  # write header only once

    print("CSV Consumer listening...")

    for msg in consumer:
        data = msg.value
        writer.writerow({
            "city": data["city"],
            "temperature": data["temperature"],
            "humidity": data["humidity"],
            "description": data["description"]
        })
        print("Written to CSV:", data)
