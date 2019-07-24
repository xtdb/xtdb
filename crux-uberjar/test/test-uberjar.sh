#!/bin/bash

# Start the uberjar in the background
test/test-start-uberjar.sh &

# Sleep for enough time to run:
# -> tests intermittently fail at 10s, safe so far at 15s, may need to increase.
sleep 15

# Cancel the process - avoid closing other java processes
jps | grep "crux" | grep ".jar" | awk '{print $1}' | xargs kill -TERM

# Remove the LOCK (this seems a dodgy thing to do?)
rm data/LOCK

exit 0
