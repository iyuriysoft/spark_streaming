echo "start zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties &# > zookeeper.log 2>&1 &

sleep 2

echo "start kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties --override delete.topic.enable=true &#> server.log 2>&1 &

sleep 2

echo "create topic: firsttopic"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic firsttopic

sleep 2

echo "kafka has next topics:"
$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper localhost:2181
