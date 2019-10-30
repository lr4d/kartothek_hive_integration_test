#!/usr/bin/env python

import os
from functools import partial

import pandas as pd
import pandas.testing as pdt
import storefact
from kartothek.io.eager import store_dataframes_as_dataset
from kartothek.serialization.testing import get_dataframe_not_nested
from pyhive import hive

VOLUME_LOCATION = "/parquet_data"


def assert_hive_compat(df, store_factory, uuid, **kwargs):
    TABLE_NAME = uuid  # Hive table name

    dm = store_dataframes_as_dataset(
        store=store_factory, dataset_uuid=uuid, dfs=[df], **kwargs
    )

    store = store_factory()

    print(f"Dataset location: {VOLUME_LOCATION}")

    # Use Pyhive to query hive
    conn = hive.Connection(host="hive-server", port=10000)
    cursor = conn.cursor()

    # TODO: test partitioned dataset
    for filepath in store.iter_keys():
        if filepath.endswith(".parquet"):
            parquet_file_parentdir = f"{VOLUME_LOCATION}/{os.path.dirname(filepath)}"
            break

    if kwargs.get("partition_on"):
        for i in kwargs.get(
            "partition_on"
        ):  # Get the parent directory of the parquet file for each column it is partitioned on
            # Note. Parquet filepath looks like: `/tmp/uuid/table/partition_col1=x/partition_col2=y/1300dadda3.parquet`
            parquet_file_parentdir = os.path.dirname(parquet_file_parentdir)

    # Create Hive table
    ## Non-nested columns not included: `np.uint64` (max value is too large for `BIGINT`)
    ## The `null` column can be specified as multiple types (at least `STRING` and `FLOAT`)
    # TODO: have a mapping from kartothek/arrow dtypes to Hive dtypes
    selected_columns_and_dtypes = """\
                bool BOOLEAN,
                bytes BINARY,
                date_ DATE,
                datetime64 BIGINT,
                float32 FLOAT,
                float64 DOUBLE,
                int8 TINYINT,
                int16 SMALLINT,
                int32 INT,
                int64 BIGINT,
                uint8 SMALLINT,
                uint16 INT,
                uint32 BIGINT,
                unicode STRING,
                null_ FLOAT"""
    # Hive allows us to only select a subset of columns to be loaded from the Parquet file
    hive_query = f"""
      CREATE external table {TABLE_NAME} (
        {selected_columns_and_dtypes}
        )
      STORED AS PARQUET
      LOCATION "{parquet_file_parentdir}"
    """

    print(f"Hive query: {hive_query}")
    cursor.execute(hive_query)

    # Get column names from query substring
    selected_columns = [
        l.strip().split(" ")[0] for l in selected_columns_and_dtypes.splitlines()
    ]
    # Read hive table into pandas
    hive_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    hive_df.columns = selected_columns
    # Pyarrow stores timestamp as microseconds from epoch, convert to date
    hive_df["datetime64"] = pd.to_datetime(
        hive_df.loc[:, "datetime64"] * 1000, unit="ns"
    )
    # Output from hive is a string, parse this to date
    hive_df["date_"] = pd.to_datetime(hive_df.loc[:, "date_"], format="%Y-%m-%d").apply(
        lambda x: x.date()
    )

    # Ignore dtype for numeric comparisons (e.g. int32 with int64)
    pdt.assert_frame_equal(df[selected_columns], hive_df, check_dtype=False)
    print(f"Test completed for the following data types: {[sc.rstrip('_') for sc in selected_columns]}")


# Create dataset on local filesystem
store_factory = partial(storefact.get_store_from_url, f"hfs://{VOLUME_LOCATION}")

df = get_dataframe_not_nested(100)
# Rename because `date` and `null` are reserved in Hive QL
df = df.rename(columns={"date": "date_", "null": "null_"})
assert_hive_compat(df, store_factory, uuid="test")