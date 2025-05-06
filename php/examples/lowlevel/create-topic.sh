#!/bin/bash

# Скрипт для создания топика
docker exec -it kafka_examples_kafka kafka-topics --create \
    --topic lowlevel-topic \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Проверка созданного топика
docker exec -it kafka_examples_kafka kafka-topics --describe \
    --topic lowlevel-topic \
    --bootstrap-server localhost:9092
