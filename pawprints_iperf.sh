#!/bin/bash
SERVER="192.168.94.3"
DURATION=300 # seconds
IPERF_LOG_PATH="/data/local/tmp/iperf_results" 
echo "Running iperf as client to server: $SERVER for $DURATION seconds."
adb shell "/data/local/tmp/iperf3 -c $SERVER -R -p 5201 -i 1 -t $DURATION --logfile $IPERF_LOG_PATH"

