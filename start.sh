#!/bin/bash

# Change the directory to the directory of this script
cd "$(dirname "$0")"

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null
then
    echo "Python 3 could not be found"
    exit
fi

# Create a Python virtual environment named 'venv' in the current directory
python3 -m venv venv

# Activate the virtual environment
# Note: Activation is shell-specific, this is for Bash
source venv/bin/activate
pip install .
mse start -s $1
