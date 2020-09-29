#!/bin/bash


if [[ -n $DEBUG ]]; then
    echo "DEBUG=$DEBUG"
    conda install -y ipython ipdb
    echo "Skipping tests and sleeping for a year"
    sleep 365d
else
    echo "Running hive compatibility test..."
    pytest -s -rA test_hive_compatibility.py
fi
