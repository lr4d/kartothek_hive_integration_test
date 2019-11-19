# Integration testing of kartothek with Hive

## Introduction
The main intention of this test is to ensure that datasets created by kartothek (using `pyarrow`'s Parquet implementation) can be read by Hive (and similar tools in the Java/Hadoop ecosystem).

This docker-compose setup creates a kartothek dataset with a number of different data types and uses Hive to read the generated Parquet files.

Note: When forking the repo, to run the tests on a periodic basis you will need to setup a cron job in travis. See:
https://docs.travis-ci.com/user/cron-jobs/#adding-cron-jobs for instructions on how to do this.

## Compatibility issues
 - Timestamps: Arrow stores timestamps in Parquet files as microseconds from epoch. This is not supported by Hive's `DATETIME`. Therefore we import the datetime column from our Parquet file with the data type `BIGINT`.
 - Unsigned integers are not supported by Hive. This is not a major issue as they can be imported as signed integers. However, since the maximum value of `np.uint64` exceeds the byte-length of `BIGINT` (64-byte signed integer), it cannot be reliably loaded.

# Partitioning
As of writing we are using Hive 2.3.2. Therefore, we manually need to execute a query so that Hive can "discover partitions" of our dataset. This is no longer required as of Hive 4.0.0 (see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-DiscoverPartitions).
## Testing setup
At the moment of writing, the tests run on Hadoop 2.7.4 and Hive 2.3.2. This deploys Hive and starts a hiveserver2 on port 10000. 
A Hive metastore is running with a connection to a PostgreSQL database.

### Testing script
The test script uses `pytest` for test execution and can be found in `test-executor/test_hive_compatibility.py`.

By executing this script, the `test-executor` service creates a kartothek data set, creates a Hive table with a schema defined according to the kartothek dataset and loads the kartothek data set into the Hive table. Then the data is extracted from the Hive table and validated against the kartothek dataset.

Currently the validation is done for the following data types: `'bool', 'bytes', 'date', 'datetime64', 'float32', 'float64', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'unicode', 'null'`.

 ## To-do's
 - Test using the latest stable version of Hive.
 - Full integration with standard kartothek tests.  At the moment of writing the tests take around 45 seconds, excluding the docker image build. The image build could be cached, and this test could be added as a job to the default kartothek testing build.
 - (?) Test reading from Presto.
 - (?) Test reading from Drill.
 - (?) Convert a Hive dataset to a kartothek dataset.

`(?)` indicates potential to-do's, which should be regarded as low priority.

## Development
In order to spin up the docker-compose cluster, 

- Build the images locally (if required) using: `make build` 
- Then bring up all required services using: `make run`
- By default a docker container exits after the main process, defined with the `CMD` instruction, exits. For local development, we therefore
run `sleep 365d` as the `CMD` instead, in order to keep the `test-executor` alive

If you want to test code interactively execute code from the `test-executor` service with the following commands:

    docker-compose exec test-executor python  # Open a shell inside `test-executor`
