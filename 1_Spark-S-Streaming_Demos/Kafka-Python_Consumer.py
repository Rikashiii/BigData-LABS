#!/usr/bin/python3

# Run the python file by ./Kafka-Python_Consumer.py

# Step 1: Import Kafka Consumer
from kafka import KafkaConsumer

# Step 2: Create a Consumer Instance
#consumer = KafkaConsumer(
#    'test_topic',
#    bootstrap_servers='localhost:9092',
#    group_id='my_group'
#)

# Step 3: Reading Messages
#for message in consumer:
#    print(f"{message.key}: {message.value}")

# Step 4: Configuring the Consumer
consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers='localhost:9092',
    group_id='my_group',
    auto_offset_reset='earliest'  # start from the earliest message
)

# Step 5: Deserializing Data
#import json
 
#consumer = KafkaConsumer(
#    'test_topic',
#    bootstrap_servers='localhost:9092',
#    auto_offset_reset='earliest',
#    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#)
 
for message in consumer:
    print(message.value)
