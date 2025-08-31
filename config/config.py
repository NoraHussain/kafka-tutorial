# config.py
import os

API_KEY = os.getenv("API_TOKEN")
BROKER_ADDRESS = "localhost:9092"
TOPIC_NAME = "weather-topic2"