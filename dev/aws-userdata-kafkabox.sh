#!/bin/bash

yum update -y

nb=$1
port=$((9092 + $nb))
zoobox_ip[1]=10.0.0.11
my_ip=$(hostname -i)

mkdir -p /stack/kafka/data

wget "http://mirrors.ukfast.co.uk/sites/ftp.apache.org/kafka/1.1.0/kafka_2.11-1.1.0.tgz" -P /stack/kafka
tar -xzf /stack/kafka/kafka_2.11-1.1.0.tgz -C /stack/kafka --strip 1
rm -f /stack/kafka/kafka_2.11-1.1.0.tgz

cd /stack/kafka

cp config/server.properties config/server$nb.properties    
sed -i 's/broker.id=0/broker.id='$nb'/g' config/server$nb.properties
sed -i 's/#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/:'$port'/g' config/server$nb.properties
sed -i 's/#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/advertised.listeners=PLAINTEXT:\/\/'$my_ip':'$port'/g' config/server$nb.properties
sed -i 's/log.dirs=\/tmp\/kafka-logs/log.dirs=\/stack\/kafka\/data\/kafka-logs'$nb'/g' config/server$nb.properties
sed -i 's/zookeeper.connect=localhost:2181/zookeeper.connect='${zoobox_ip[1]}':2181/g' config/server$nb.properties

/stack/kafka/bin/kafka-server-start.sh /stack/kafka/config/server$nb.properties > /stack/kafka/data/kafka-server$nb.out &

chmod -R 777 /stack
