# spark_streaming
Spark Structured Streaming and DStream examples

### There are two options to get source streaming data:
1. get from json-file (StreamingFile.java)
2. get from Kafka (StreamingKafka.java)

Source data format like that:

{"ip": "172.10.2.125", "unix_time": 1543170426, "type": "view", "category_id": 1005},

{"ip": "172.10.3.135", "unix_time": 1543170426, "type": "click", "category_id": 1007},

...

Signs of bot's activity in our case:
1. Enormous event rate, e.g. more than 59 request in 2 minutes.
2. High difference between click and view events, e.g. (clicks/views) more than 3.5. Correctly process cases when there is no views.
3. Looking for many categories during the period, e.g. more than 15 categories in 2 minutes.

Starts real-time emulation (transfer data from json-file to Kafka (to use in StreamingKafka.java):
```
> java -jar stopbot-0.0.1-SNAPSHOT-jar-with-dependencies.jar 1 300s2.json localhost:9092 firsttopic
```

