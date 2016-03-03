#!/usr/bin/env bash
sudo -u hdfs spark-submit --master yarn-client --executor-memory 2G --num-executors 3 --class big.data.cable.tv.STBStatistics /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar /home/lnovikova/projects/cableTv/statistic > /home/lnovikova/projects/cableTv/statistic/cableTvStreaming.log

sudo -u hdfs spark-submit --master local[4] --executor-memory 2G --num-executors 3 --class big.data.cable.tv.STBStatistics /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar /home/lnovikova/projects/cableTv/statistic > /opt/lnovikova/cabelTV/statistic/cableTvStreaming.log


sudo -u hdfs spark-submit --master yarn --deploy-mode cluster --executor-memory 2G --num-executors 3 --class big.data.cable.tv.STBStatistics /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar /home/lnovikova/projects/cableTv/statistic > /opt/lnovikova/cabelTV/statistic/cableTvStreaming.log

sudo -u hdfs spark-submit --master yarn-cluster --deploy-mode cluster --executor-memory 2G --num-executors 3 --class big.data.cable.tv.STBStatistics /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar /home/lnovikova/projects/cableTv/statistic > /opt/lnovikova/cabelTV/statistic/cableTvStreaming.log


sudo -u hdfs spark-submit --master local[4] --executor-memory 2G --num-executors 3 /opt/lnovikova/cabelTV/streaming/cabletv_2.10-1.0.jar --class big.data.cable.tv.KafkaStreamProcessing

CREATE TABLE IF NOT EXISTS Q (mac: String,cluster: Int,pvod: Double)