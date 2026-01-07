#!/bin/bash

# Wrapper script to stop Doris FE processes
# This script provides multiple ways to stop FE processes

set -e

echo "Stopping Doris FE processes..."

# Method 1: Try graceful shutdown first
echo "Attempting graceful shutdown..."
if pgrep -f "DorisFE" > /dev/null; then
    pkill -TERM -f "DorisFE"
    
    # Wait up to 10 seconds for graceful shutdown
    for i in {1..10}; do
        if ! pgrep -f "DorisFE" > /dev/null; then
            echo "Doris FE stopped gracefully."
            exit 0
        fi
        echo "Waiting for graceful shutdown... ($i/10)"
        sleep 1
    done
    
    echo "Graceful shutdown timed out, trying force kill..."
    
    # Method 2: Force kill if graceful shutdown fails
    if pgrep -f "DorisFE" > /dev/null; then
        pkill -KILL -f "DorisFE"
        sleep 2
        
        if ! pgrep -f "DorisFE" > /dev/null; then
            echo "Doris FE force stopped."
        else
            echo "Warning: Some DorisFE processes may still be running."
            echo "Running processes:"
            pgrep -f "DorisFE" | while read pid; do
                echo "  PID: $pid"
            done
        fi
    fi
else
    echo "No DorisFE processes found running."
fi

echo "Stop script completed."

