#!/usr/bin/env bash
/usr/bin/kafka-console-consumer --zookeeper 192.168.1.31:2181 --topic StbFailure
spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-cluster --executor-memory 2G --num-executors 3 /opt/projects/cableTv/CableTv-assembly-1.0.jar bigdata1.nnstu.com:9092 StbStructuredMessage StbStream
