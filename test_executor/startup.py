#!/usr/bin/env python

import storefact
from functools import partial
from tempfile import TemporaryDirectory
import numpy as np
import pandas as pd
from kartothek.io.eager import store_dataframes_as_dataset

VOLUME_LOCATION = "/parquet_data"

# Create dataset on local filesystem
tmpdir = TemporaryDirectory().name
store_factory = partial(storefact.get_store_from_url, f"hfs://{VOLUME_LOCATION}")
from kartothek.serialization.testing import get_dataframe_not_nested

df = get_dataframe_not_nested(100)
# Rename because `date` and `null` are reserved in Hive QL
df = df.rename(columns={"date": "date_", "null": "null_"})
uuid = "test"

dm = store_dataframes_as_dataset(store=store_factory, dataset_uuid=uuid, dfs=[df])

store = store_factory()

print(f"Dataset location: {VOLUME_LOCATION}")


# Use Pyhive to query hive
from pyhive import hive

conn = hive.Connection(host="hive-server", port=10000)
cursor = conn.cursor()
TABLE_NAME = "ktk"
import os

for filepath in store.iter_keys():
    if filepath.endswith(".parquet"):
        parquet_file_parentdir = f"{VOLUME_LOCATION}/{os.path.dirname(filepath)}"

# Create Hive table
cursor.execute(f"DROP TABLE {TABLE_NAME}")  # TODO: remove
# Non-nested columns not included: `np.uint64` (max value is too large for `BIGINT`)
# The `null` column can be specified as multiple types (at least `STRING` and `FLOAT`)
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
# We can select the columns we want to load from the Parquet file, not all need to be specified
cursor.execute(
    f"""
  CREATE external table {TABLE_NAME} (
    {selected_columns_and_dtypes}
    )
  STORED AS PARQUET
  LOCATION "{parquet_file_parentdir}"
"""
)
selected_columns = [
    l.strip().split(" ")[0] for l in selected_columns_and_dtypes.splitlines()
]

hive_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
hive_df.columns = selected_columns
hive_df["datetime64"] = pd.to_datetime(
    hive_df.loc[:, "datetime64"] * 1000, unit="ns"
)  # Loaded timestamp is in microseconds
hive_df["date_"] = pd.to_datetime(hive_df.loc[:, "date_"], format="%Y-%m-%d").apply(
    lambda x: x.date()
)  # Output from hive is a string

import pandas.testing as pdt

pdt.assert_frame_equal(
    df[selected_columns], hive_df, check_dtype=False
)  # Ignore dtype for numeric comparisons
print(f"Test completed for the following data types: {selected_columns}")