#!/bin/bash

# This script is designed for demo4.py, demo5.py, demo8.py
# It will create a kafka topic test_topic
# Prerequisit is zookeeper is running, Kafka broker is running
# Run Zookeeper by a command ~/run-kafka_zookeeper_server.sh -s start
# Run Kafka broker by a command ~/run-kafka_server.sh -s start

source ~/unset_jupyter.sh
jps
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic test_topic --delete
echo "-----Kafka topic test_topic deleted-----"
kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test_topic
kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
echo "-----Kafka topic test_topic created-----"

