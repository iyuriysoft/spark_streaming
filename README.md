# spark_streaming
Spark Structured Streaming and DStream examples

## Sample task
Need to detect bot traffic.
The input data is a log with requests from users each of them contains fields like ip address, url or category page, timestamp, 'click' or 'view' type and so on so forth. 

In the concrete example (in test_data), the source data contains the bots that execute requests every 2 seconds.
 
Source data format looks like that:

{"ip": "172.10.2.125", "unix_time": 1543170426, "type": "view", "category_id": 1005},

{"ip": "172.10.3.135", "unix_time": 1543170426, "type": "click", "category_id": 1007},

...

Signs of bot's activity in our case:
1. Enormous event rate, e.g. more than 59 request in 2 minutes.
2. High difference between click and view events, e.g. (clicks/views) more than 2.5. Correctly process cases when there is no views.
3. Looking for many categories during the period, e.g. more than 15 categories in 2 minutes.


To simulate receiving data there is File2KafkaRealtime class.
Start real-time simulation - transfer data from json-file to Kafka:
```
> java -jar stopbot-0.0.1-SNAPSHOT-jar-with-dependencies.jar 1 300s2.json localhost:9092 firsttopic
```

## There are two ways to decide the task

### A. Structured Streaming
**There are two options to get source streaming data:**
1. get data from json-file source (StreamingFile)
2. get data from Kafka source (StreamingKafka)

### B. DStream
**There are two approaches to achieve the same results:**
1. use DStream technology (StreamingDStream)
2. use RDD, just transform DStream to set of RDD and analyze it (StreamingRDD)


