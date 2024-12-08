# DistributedSystemProject
Project for the first semester course in Distributed System 


# Develop with Kafka and Pyspark locally 

```
docker run -d --name zookeeper -p 2181:2181     -e ZOOKEEPER_CLIENT_PORT=2181     confluentinc/cp-zookeeper
c43226323e635b382803bead31ad28a5714624350b7f57b26653878a413e7d72
docker run -d --name kafka -p 9092:9092 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
    --link zookeeper \
    confluentinc/cp-kafka

```

## Create a topic 

```
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## List topics 

```
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

This should install and build locally.