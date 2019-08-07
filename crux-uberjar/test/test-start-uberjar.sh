#!/bin/bash

# Write names of .jar to tempory file
lein uberjar > tmp-jar

# Assign to variable standalone to variable
JAR=$(awk 'END {print $NF}' tmp-jar)

# Remove temp file
rm tmp-jar

echo "STARTING $JAR"

# Start the uberjar - print output to file
java -jar ${JAR} > target/uberjar-test-results

echo "Finished"
