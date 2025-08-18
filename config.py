import os
from dotenv import load_dotenv

load_dotenv()

URL = os.environ.get("HOST")
TOKEN = os.environ.get("TOKEN")
ORG = os.environ.get("ORG")
BROKER = os.environ.get("BROKER")
TOPIC = os.environ.get("TOPIC")
PORT = os.environ.get("PORT")
CLIENT_ID = os.environ.get("CLIENT_ID")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
TELEGRAF=os.environ.get("TELEGRAF")