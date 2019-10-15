# Integration testing of kartothek with Hive
This docker-compose creates a kartothek dataset with a number of different data types and loads this into Hive.
The test script can be found in `test-executor/startup.py`.

This docker is based on Hadoop 2.7.4 and Hive 2.3.2. This deploys Hive and starts a hiveserver2 on port 10000. 
Metastore is running with a connection to postgresql database.

Inorder to spin up the docker load the base hadoop image using the `before_install.sh` followed by

    docker-compose up -d

This builds all the hadoop services like namenamde, datanode, hive server, hive metastore along with the test service for kartothek.

# Testing
The Test-executor service creates a kartothek data set and loads it into HIVE table. Then the data is extracted from hive table and validated against the kartothek dataset datatypes.
Currently the validation is done for `'bool', 'bytes', 'date_', 'datetime64', 'float32', 'float64', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'unicode', 'null_'` datatypes.
    
    $ docker-compose exec test_executor bash
    # sh /usr/local/bin/startup.py

# Compatibility issues

 - Timestamps: Arrow stores timestamps in Parquet files as microseconds from epoch. This is not supported by Hive's `DATETIME`. Therefore we import the datetime column from our Parquet file with the data type `BIGINT`.
 - Unsigned integers are not supported by Hive. This is not a major issue as they can be imported as signed integers. However, since the maximum value of `np.uint64` exceeds the byte-length of `BIGINT` (64-byte signed integer), it cannot be reliably loaded.

 # To-do's

 - Read a kartothek dataset from Hive which consists of more than a single file.
 - Use a partitioned kartothek dataset.
 - Test using the latest stable version of Hive.
 - (?) Test reading from Presto.
 - (?) Test reading from Drill.
 - (?) Convert a Hive dataset to a kartothek dataset.


