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

 ```bash
redis-cli monitor
```

```bash
redis-cli
```

```bash
# Displaying all availables keys
127.0.0.1:6379> keys *

# Displaying specific keys matched with pattern
127.0.0.1:6379> keys NETFLIX:11610:*
 1) "NETFLIX:11610:2004-03"
 2) "NETFLIX:11610:2002-07"
 3) "NETFLIX:11610:2003-05"
 4) "NETFLIX:11610:2002-08"
 5) "NETFLIX:11610:2003-03"
 6) "NETFLIX:11610:2002-12"
 7) "NETFLIX:11610:2003-11"
 8) "NETFLIX:11610:2002-11"
 9) "NETFLIX:11610:2002-10"
10) "NETFLIX:11610:2003-06"
11) "NETFLIX:11610:2003-04"
12) "NETFLIX:11610:2003-02"
13) "NETFLIX:11610:2004-02"
14) "NETFLIX:11610:2003-01"
15) "NETFLIX:11610:2003-10"
16) "NETFLIX:11610:2003-07"
17) "NETFLIX:11610:2004-01"
18) "NETFLIX:11610:2002-06"
19) "NETFLIX:11610:2003-09"
20) "NETFLIX:11610:2003-08"
21) "NETFLIX:11610:2003-12"
22) "NETFLIX:11610:2002-09"

# Displaying values of specific key
127.0.0.1:6379> hgetall NETFLIX:11610:2004-03
 1) "sum_of_rates"
 2) "84"
 3) "number_of_rates"
 4) "25"
 5) "month"
 6) "2004-03"
 7) "film_id"
 8) "11610"
 9) "number_of_unique_users_rates"
10) "26"
11) "film_title"
12) "A Brilliant Madness: American Experience"
```