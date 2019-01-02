echo "kafka has next topics:"
$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper localhost:2181

sleep 2

echo "delete topic: firsttopic"
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic firsttopic

echo "stop Kafka..."
$KAFKA_HOME/bin/kafka-server-stop.sh

sleep 2

echo "stop Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-stop.sh