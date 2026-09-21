docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic events-deduped \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists