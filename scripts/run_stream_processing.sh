#!/usr/bin/env bash
spark-submit --class big.data.cable.tv.KafkaStreamProcessing --master yarn-client --executor-memory 2G --num-executors 3 /opt/projects/cableTv/cabletv_2.10-1.0.jar