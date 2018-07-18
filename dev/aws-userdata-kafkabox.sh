#!/bin/bash

NB=1
KAFKA_BOX_IP[1]=10.0.0.11
KAFKA_BOX_IP[2]=10.0.0.12
KAFKA_BOX_IP[3]=10.0.0.13

yum update -y

mkdir -p /stack/kafka/data
mkdir -p /stack/zookeeper/data
mkdir -p /stack/zookeeper/config

wget "http://mirrors.ukfast.co.uk/sites/ftp.apache.org/kafka/1.1.0/kafka_2.11-1.1.0.tgz" -P /stack/kafka
tar -xzf /stack/kafka/kafka_2.11-1.1.0.tgz -C /stack/kafka --strip 1
rm /stack/kafka/kafka_2.11-1.1.0.tgz

cd /stack/zookeeper

cat << EOF > config/zookeeper-cluster.properties
clientPort=2181
dataDir=/stack/zookeeper/data
tickTime=2000
initLimit=5
syncLimit=2
server.1=${KAFKA_BOX_IP[1]}:2888:3888
server.2=${KAFKA_BOX_IP[2]}:2888:3888
server.3=${KAFKA_BOX_IP[3]}:2888:3888
EOF
sed -i 's/^server.'$NB'.*/server.'$NB'=0.0.0.0:2888:3888/g' config/zookeeper-cluster.properties

echo $NB >> data/myid

/stack/kafka/bin/zookeeper-server-start.sh /stack/zookeeper/config/zookeeper-cluster.properties > /stack/zookeeper/data/zookeeper.out &

cd /stack/kafka

PORT=$((9092 + $NB))

cp config/server.properties config/server$NB.properties    
sed -i 's/broker.id=0/broker.id='$NB'/g' config/server$NB.properties
sed -i 's/#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/:'$PORT'/g' config/server$NB.properties
sed -i 's/#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/advertised.listeners=PLAINTEXT:\/\/'${KAFKA_BOX_IP[$NB]}':'$PORT'/g' config/server$NB.properties
sed -i 's/log.dirs=\/tmp\/kafka-logs/log.dirs=\/stack\/kafka\/data\/kafka-logs'$NB'/g' config/server$NB.properties

/stack/kafka/bin/kafka-server-start.sh /stack/kafka/config/server$NB.properties > /stack/kafka/data/kafka-server$NB.out &

chmod -R 777 /stack
