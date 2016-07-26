#!/bin/bash

# dump rss messages into a kafka cluster on CDH 5.3 using the rss.perl script to 
# access reuters rss messages and the kafka kafka-console-producer script to 
# pipe them in.

ZOOKEEPER="hc2r1m2:2181,hc2r1m3:2181,hc2r1m4:2181/kafka"
TOPIC="rss"
GROUP="group1"


# list the topics 

echo ""
echo "================================"
echo " Kafka Topics "
echo "================================"

/usr/bin/kafka-topics --list --zookeeper $ZOOKEEPER 

# list the offsets 

echo ""
echo "================================"
echo " Kafka Offsets "
echo "================================"

/usr/bin/kafka-consumer-offset-checker \
  --group $GROUP \
  --topic $TOPIC \
  --zookeeper $ZOOKEEPER 

echo ""

