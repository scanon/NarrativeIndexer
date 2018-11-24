#!/bin/sh

echo "Start up test instances"

echo "Start Elasticsearch"
docker run -d --name es5 -v es5:/usr/share/elasticsearch/data/ -e "xpack.security.enabled=false" -e "ES_JAVA_OPTS=-Xms512m -Xmx512m"  -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:5.6.9

echo "Start Zookeeper"
docker run -d \
    --name=zookeeper \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:5.0.0
sleep 2
echo "Start Kafka"
docker run -d \
    --name=kafka \
    --link zookeeper \
    -p 9092:9092 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e "AFKA_HEAP_OPTS=-Xmx512m -Xms512m" \
    confluentinc/cp-kafka:5.0.0
