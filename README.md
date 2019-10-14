# Integration testing of kartothek with Hive
This docker-compose creates a kartothek dataset with a number of different data types and loads this into Hive.
The test script can be found in `test-executor/startup.py`.

# Compatibility issues

 - Timestamps: Arrow stores timestamps in Parquet files as microseconds from epoch. This is not supported by Hive's `DATETIME`. Therefore we import the datetime column from our Parquet file with the data type `BIGINT`.
 - Unsigned integers are not supported by Hive. This is not a major issue as they can be imported as signed integers. However, since the maximum value of `np.uint64` exceeds the byte-length of `BIGINT` (64-byte signed integer), it cannot be reliably loaded.