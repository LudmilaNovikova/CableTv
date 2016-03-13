#!/usr/bin/env bash
sudo -u hdfs spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-client --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar #> /opt/lnovikova/cabelTV/streaming/log/cableTvStreaming.log
 sudo -u hdfs spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-client --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar
spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master local[4] --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar | tee /home/lnovikova/projects/cableTv/log/cableTvStreaming.log

/usr/bin/kafka-console-consumer --zookeeper 192.168.1.31:2181 --topic SbtFailure --from-beginning
spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-cluster --executor-memory 2G --num-executors 3 --jars /opt/projects/cableTv/kafka-clients-0.8.2.0.jar /opt/projects/cableTv/cabletv_2.10-1.0.jar
spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-cluster --executor-memory 2G --num-executors 3 /opt/projects/cableTv/CableTv-assembly-1.0.jar
