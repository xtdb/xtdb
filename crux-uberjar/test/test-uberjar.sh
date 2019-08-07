#!/bin/bash

# Start the uberjar in the background
test/test-start-uberjar.sh &

# Sleep for enough time to run:
# -> tests intermittently fail at 15s, safe so far at 20s, may need to increase.
sleep 60

# Cancel the process - avoid closing other java processes
jps | grep "crux" | grep ".jar" | awk '{print $1}' | xargs kill -TERM

# Remove the LOCK (this seems a dodgy thing to do?)
rm -f data/LOCK

exit 0
