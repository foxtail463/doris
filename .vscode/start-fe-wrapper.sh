#!/bin/bash

# Wrapper script to start Doris FE for debugging
# This script starts the FE process and waits for it to be ready for debugging

set -e

# Get the directory of this script (should be .vscode)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the workspace root (parent of .vscode, which is now the Doris root)
WORKSPACE_ROOT="$(dirname "${SCRIPT_DIR}")"

# Path to the FE directory
FE_DIR="${WORKSPACE_ROOT}/output/fe"
FE_BIN_DIR="${FE_DIR}/bin"
FE_START_SCRIPT="${FE_BIN_DIR}/start_fe.sh"

echo "Starting Doris FE for debugging..."
echo "FE directory: ${FE_DIR}"
echo "Start script: ${FE_START_SCRIPT}"

# Check if the start script exists
if [[ ! -f "${FE_START_SCRIPT}" ]]; then
    echo "Error: start_fe.sh not found at ${FE_START_SCRIPT}"
    echo "Please make sure Doris FE is built and output directory exists."
    exit 1
fi

# Change to FE directory
cd "${FE_DIR}"

# Start FE in daemon mode so it runs in background and can be attached to
"${FE_START_SCRIPT}" --daemon

# Wait a moment for the process to start
sleep 15

# Check if the process is running
if pgrep -f "DorisFE" > /dev/null; then
    echo "Doris FE started successfully and is running."
    echo "Process ID(s): $(pgrep -f 'DorisFE' | tr '\n' ' ')"
    echo "Debug port should be available on localhost:5005"
    exit 0
else
    echo "Error: Doris FE failed to start or exited immediately."
    echo "Check the logs in ${FE_DIR}/log/ for more information."
    exit 1
fi

