# SparkStructeredStreaming with Redis

Streaming Processing with Spark Structured Streaming with Redis for Netflix Prize Data 

Source of data: https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data?select=README


# Run the project

## Step 1

Make sure you have installed ```redis-server``` and configured files ```redis.conf``` and then check if redis client is working with this command in terminal

```console
redis-cli
ping
```
If the answer will be ```PONG``` it means all is set up.

## Step 2

Load all data, jar and shell files.

## Step 3

Make your shell files executable with this comand:

```shell
chmod +x *.sh
```

In first terminal run

```console
./produce.sh
```

 to start Kafka producer

 In second terminal run

 ```console
./process.sh
```

 to start streaming processing Kafka topics.

 ## Step 4

 After some time you can check if data is properly writed to sink (Redis) with this commands in terminal

 ```console
redis-cli monitor
 ```

  ```console
hgetall key
 ```

  ```console
keys PATTERN
 ```

  ```console
keys ANOMALIES:*
 ```


  ```console
keys NETFLIX:*
 ```