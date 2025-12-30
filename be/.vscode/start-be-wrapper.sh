#!/bin/bash

# Wrapper script to start Doris BE for debugging
# This script starts the BE process and waits for it to be ready for debugging

set -e
export SKIP_CHECK_ULIMIT=true

# Get the directory of this script (should be .vscode)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the workspace root (parent of .vscode)
WORKSPACE_ROOT="$(dirname "${SCRIPT_DIR}")"
# Get the Doris root (parent of workspace)
DORIS_ROOT="$(dirname "${WORKSPACE_ROOT}")"

# Path to the BE directory
BE_DIR="${DORIS_ROOT}/output/be"
BE_BIN_DIR="${BE_DIR}/bin"
BE_START_SCRIPT="${BE_BIN_DIR}/start_be.sh"

echo "Starting Doris BE for debugging..."
echo "BE directory: ${BE_DIR}"
echo "Start script: ${BE_START_SCRIPT}"

# Check if the start script exists
if [[ ! -f "${BE_START_SCRIPT}" ]]; then
    echo "Error: start_be.sh not found at ${BE_START_SCRIPT}"
    echo "Please make sure Doris BE is built and output directory exists."
    exit 1
fi

# Change to BE directory
cd "${BE_DIR}"

# Start BE in daemon mode so it runs in background and can be attached to
"${BE_START_SCRIPT}" --daemon

# Wait a moment for the process to start
sleep 2

# Check if the process is running
if pgrep -f "doris_be" > /dev/null; then
    echo "Doris BE started successfully and is running."
    echo "Process ID(s): $(pgrep -f 'doris_be' | tr '\n' ' ')"
    exit 0
else
    echo "Error: Doris BE failed to start or exited immediately."
    echo "Check the logs in ${BE_DIR}/log/ for more information."
    exit 1
fi 