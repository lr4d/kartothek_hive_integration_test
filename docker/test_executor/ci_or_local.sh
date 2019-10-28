#!/bin/bash

echo "FOR_CI is: $FOR_CI"

if [ "$FOR_CI" = "True" ]; then
    echo "Running hive compatibility test..."
    startup.py
else
    echo "Skipping tests and sleeping for a year"
    sleep 365d
fi
