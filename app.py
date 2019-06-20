#!/bin/python3

from flask import Flask
from kafka import KafkaProducer
import json
app = Flask(__name__)

@app.route("/ping")
def ping():
    return "Pong"


MOCKED_VALUE = {
  "value": {
    "XID": "13846952522",
    "SQL_OPERATION": "INSERT",
    "ID": 79461,
    "ACTIVITY_DESCRIPTION": "[New Activity From Contract MS] - Contractual Payment and Customer Deposit Test",
    "CO_CONTRACT_ID": {
      "int": 73802
    },
    "CO_ACTIVITY_TYPE_ID": 6,
    "PD_PERSON_ID": 105896
  }
}

MOCKED_KEY = b'79461'

@app.route("/send")
def send():
    producer = KafkaProducer(bootstrap_servers='172.16.0.81:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    future = producer.send('GVPOLL_CONTRACTS_CO_ACTIVITIES',
                           value=MOCKED_VALUE, key=MOCKED_KEY)
    result = future.get(timeout=60)
    print(result)
    return 'Success'
