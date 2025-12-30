#!/bin/bash

# Wrapper script to stop Doris BE processes
# This script provides multiple ways to stop BE processes

set -e

echo "Stopping Doris BE processes..."

# Method 1: Try graceful shutdown first
echo "Attempting graceful shutdown..."
if pgrep -f "doris_be" > /dev/null; then
    pkill -TERM -f "doris_be"
    # Wait up to 10 seconds for graceful shutdown
    for i in {1..60}; do
        if ! pgrep -f "doris_be" > /dev/null; then
            echo "Doris BE stopped gracefully."
            exit 0
        fi
        echo "Waiting for graceful shutdown... ($i/60)"
        sleep 1
    done
    
    echo "Graceful shutdown timed out, trying force kill..."
    
    # Method 2: Force kill if graceful shutdown fails
    if pgrep -f "doris_be" > /dev/null; then
        pkill -KILL -f "doris_be"
        sleep 2
        
        if ! pgrep -f "doris_be" > /dev/null; then
            echo "Doris BE force stopped."
        else
            echo "Warning: Some doris_be processes may still be running."
            echo "Running processes:"
            pgrep -f "doris_be" | while read pid; do
                echo "  PID: $pid"
            done
        fi
    fi
else
    echo "No doris_be processes found running."
fi

echo "Stop script completed." 