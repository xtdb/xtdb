#/usr/bin/env bash

cp target/crux-console.jar ebs/ebs-uber.jar

# java -jar target/crux-console.jar

(
cd ebs
zip ebs-upload.zip ebs-uber.jar
zip -r ebs-upload.zip .ebextensions
)

# ./bin/capsule -m edge.main -A:prod ebs/ebs-uber.jar && cd ebs && zip ebs-upload.zip ebs-uber.jar && zip -r ebs-upload.zip .ebextensions
