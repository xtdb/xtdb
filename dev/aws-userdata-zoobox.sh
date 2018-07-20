#!/bin/bash

yum update -y

nb=$1
zoobox_ip[1]=10.0.0.11
zoobox_ip[2]=10.0.0.12
zoobox_ip[3]=10.0.16.11
zoobox_ip[4]=10.0.16.12
zoobox_ip[5]=10.0.32.11

mkdir -p /stack/kafka/data
mkdir -p /stack/zookeeper/data
mkdir -p /stack/zookeeper/config

wget "http://mirrors.ukfast.co.uk/sites/ftp.apache.org/kafka/1.1.0/kafka_2.11-1.1.0.tgz" -P /stack/kafka
tar -xzf /stack/kafka/kafka_2.11-1.1.0.tgz -C /stack/kafka --strip 1
rm -f /stack/kafka/kafka_2.11-1.1.0.tgz

cd /stack/zookeeper

cat << EOF > config/zookeeper-cluster.properties
clientPort=2181
dataDir=/stack/zookeeper/data
tickTime=2000
initLimit=5
syncLimit=2
server.1=${zoobox_ip[1]}:2888:3888
server.2=${zoobox_ip[2]}:2888:3888
server.3=${zoobox_ip[3]}:2888:3888
server.4=${zoobox_ip[4]}:2888:3888
server.5=${zoobox_ip[5]}:2888:3888
EOF
sed -i 's/^server.'$nb'.*/server.'$nb'=0.0.0.0:2888:3888/g' config/zookeeper-cluster.properties

echo $nb >> data/myid

/stack/kafka/bin/zookeeper-server-start.sh /stack/zookeeper/config/zookeeper-cluster.properties > /stack/zookeeper/data/zookeeper.out &
