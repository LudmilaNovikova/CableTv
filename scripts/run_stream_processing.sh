#!/usr/bin/env bash
java -cp /opt/lnovikova/cabelTV/streaming/big-data-cable-emulator-1.0.jar big.data.cable.producer.CableKafkaProducer sandbox.hortonworks.com:6667 sandbox.hortonworks.com:2181  /opt/lnovikova/cabelTV/producer/data/cont_cut_10
