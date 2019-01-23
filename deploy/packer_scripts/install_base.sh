sudo yum update -y

sudo mkdir -p /stack/kafka/data
sudo mkdir -p /stack/zookeeper/data
sudo mkdir -p /stack/zookeeper/config

sudo wget "http://mirrors.ukfast.co.uk/sites/ftp.apache.org/kafka/1.1.0/kafka_2.11-1.1.0.tgz" -P /stack/kafka
sudo tar -xzf /stack/kafka/kafka_2.11-1.1.0.tgz -C /stack/kafka --strip 1
sudo rm -f /stack/kafka/kafka_2.11-1.1.0.tgz

sudo yum remove -y java-1.7.0-openjdk-1.7.0.181-2.6.14.8.80.amzn1.x86_64
sudo yum install -y java-1.8.0-openjdk-devel
sudo yum install -y git wget
sudo wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
sudo chmod a+x lein
sudo mv lein /bin/

sudo yum install -y ruby

cd /home/ec2-user
wget https://aws-codedeploy-eu-west-1.s3.amazonaws.com/latest/install
chmod +x ./install
sudo ./install auto
