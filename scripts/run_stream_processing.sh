#!/usr/bin/env bash
sudo -u hdfs spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-client --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar #> /opt/lnovikova/cabelTV/streaming/log/cableTvStreaming.log
 sudo -u hdfs spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-client --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar
spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master local[4] --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar | tee /home/lnovikova/projects/cableTv/log/cableTvStreaming.log
