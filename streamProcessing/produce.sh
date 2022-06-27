#!/bin/sh

# usage : ./produce.sh <path_to_kafka> <server/cluster>
# for example "/path/to/kafka/kafka_2.11-0.9.0.0/libs" localhost:9092

echo "usage: ./produce.sh <path_to_kafka> <server/cluster>"

java -cp "$1/*:KafkaProducer.jar" \
com.example.bigdata.TestProducer \
netflix-prize-data \
10 \
netflix \
1 \
"$2"