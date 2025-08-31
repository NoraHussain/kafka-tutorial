# mongo_consumer.py


from kafka import KafkaConsumer
import json
from pymongo import MongoClient

import os 
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))) 

import config
consumer = KafkaConsumer(
    config.TOPIC_NAME,
    bootstrap_servers=config.BROKER_ADDRESS,
    group_id="mongo-group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

client = MongoClient("mongodb://localhost:27017/")
db = client["weatherData"]
collection = db["weather"]

print("MongoDB Consumer listening...")

for msg in consumer:
    data = msg.value
    collection.insert_one(data)
    print("Inserted into MongoDB:", data)
