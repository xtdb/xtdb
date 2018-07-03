#!/bin/bash

PRIVATE_IP[1]=172.31.0.11
PRIVATE_IP[2]=172.31.0.12
PRIVATE_IP[3]=172.31.0.13

yum update -y

mkdir -p /keys
mkdir -p /stack

yum remove -y java-1.7.0-openjdk-1.7.0.181-2.6.14.8.80.amzn1.x86_64
yum install -y java-1.8.0-openjdk-devel
yum install -y git
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+x lein
mv lein /bin/

cat << EOF > /keys/aws-ec2-rsa
PRIVATE KEY GOES HERE
EOF
chmod 700 /keys/aws-ec2-ssh
eval "$(ssh-agent -s)"
ssh-add /keys/aws-ec2-ssh

git clone git@github.com:juxt/crux.git /stack/crux

cd /stack/crux
nohup lein run -b ${PRIVATE_IP[1]}:9093 > /stack/crux/data/crux.out &

chmod -R 777 /stack
