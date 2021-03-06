#!/bin/bash

echo "RUN_ON_TRAVIS is: $RUN_ON_TRAVIS"

if [ "$RUN_ON_TRAVIS" = "True" ]; then
    echo "Running hive compatibility test..."
    pytest -s -rA test_hive_compatibility.py
else
    echo "Skipping tests and sleeping for a year"
    sleep 365d
fi
