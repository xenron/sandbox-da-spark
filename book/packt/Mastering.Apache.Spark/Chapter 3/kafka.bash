#!/bin/bash

# dump rss messages into a kafka cluster on CDH 5.3 using the rss.perl script to 
# access reuters rss messages and the kafka kafka-console-producer script to 
# pipe them in.

BROKER_LIST="hc2r1m1:9092,hc2r1m2:9092,hc2r1m3:9092,hc2r1m4:9092"
TOPIC="rss"

./rss.perl | /usr/bin/kafka-console-producer --broker-list $BROKER_LIST --topic $TOPIC

