#!/bin/sh

# Argumenty: <topic> <server> <moviesFilePath> <netflixTrigger> <anomalyTrigger> <anomalyWindow> <anomalyTresh1> <anomalyTresh2>"

# usage: ./process.sh <server/cluster>
# for example localhost:9092

echo "usage: ./process.sh <server/cluster>"

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
--class Main --master local[*] netflix.jar \
netflix \
"$1" \
movie_titles.csv \
"10 seconds" \
"10 seconds" \
"30 days" \
100 \
4
