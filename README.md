# Integration testing of kartothek with Hive

## Introduction
The main intention of this test is to ensure that datasets created by kartothek (using `pyarrow`'s Parquet implementation) can be read by Hive (and similar tools in the Java/Hadoop ecosystem).

This docker-compose setup creates a kartothek dataset with a number of different data types and uses Hive to read the generated Parquet files.

## Compatibility issues
 - Timestamps: Arrow stores timestamps in Parquet files as microseconds from epoch. This is not supported by Hive's `DATETIME`. Therefore we import the datetime column from our Parquet file with the data type `BIGINT`.
 - Unsigned integers are not supported by Hive. This is not a major issue as they can be imported as signed integers. However, since the maximum value of `np.uint64` exceeds the byte-length of `BIGINT` (64-byte signed integer), it cannot be reliably loaded.

## Testing setup
At the moment of writing, the tests run on Hadoop 2.7.4 and Hive 2.3.2. This deploys Hive and starts a hiveserver2 on port 10000. 
A Hive metastore is running with a connection to a PostgreSQL database.

### Testing script
The test script can be found in `test-executor/startup.py`.

By executing this script, the `test-executor` service creates a kartothek data set, creates a Hive table with a schema defined according to the kartothek dataset and loads the kartothek data set into the Hive table. Then the data is extracted from the Hive table and validated against the kartothek dataset.

Currently the validation is done for the following data types: `'bool', 'bytes', 'date', 'datetime64', 'float32', 'float64', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'unicode', 'null'`.

 ## To-do's
 - Read a kartothek dataset from Hive which consists of more than a single file.
 - Use a partitioned kartothek dataset.
 - Test using the latest stable version of Hive.
 - (?) Test reading from Presto.
 - (?) Test reading from Drill.
 - (?) Convert a Hive dataset to a kartothek dataset.

`(?)` indicates potential to-do's, which should be regarded as low priority.

## Development
In order to spin up the docker-compose cluster, build the images using the `before_install.sh` script and then execute:

    docker-compose up -d

This initializes the necessary hadoop services such as a namenode, datanode, hive server, hive metastore; along with the service that will execute the test (`test-executor`).

Use the script `utils/restart.sh` to restart the docker-compose cluster.

If you want to test code interactively, add the line `command: sleep 365d` in `docker-compose.yml` under `test-executor` (this will make the service not execute the default `startup.py` script, and instead remain idle).
Then, restart the cluster (`utils/restart.sh`) and execute code from the `test-executor` service with the following commands:

    docker-compose exec test-executor bash  # Open a shell inside `test-executor`
    python  # Open a Python interpreter

    



