#!/usr/bin/env python

import os
from functools import partial
from tempfile import TemporaryDirectory

import pandas as pd
import pandas.testing as pdt
import storefact
from kartothek.io.eager import store_dataframes_as_dataset
from kartothek.serialization.testing import get_dataframe_not_nested
from pyhive import hive


VOLUME_LOCATION = "/parquet_data"

# Create dataset on local filesystem
tmpdir = TemporaryDirectory().name
store_factory = partial(storefact.get_store_from_url, f"hfs://{VOLUME_LOCATION}")

df = get_dataframe_not_nested(100)
# Rename because `date` and `null` are reserved in Hive QL
df = df.rename(columns={"date": "date_", "null": "null_"})
uuid = "test"

dm = store_dataframes_as_dataset(store=store_factory, dataset_uuid=uuid, dfs=[df])

store = store_factory()

print(f"Dataset location: {VOLUME_LOCATION}")


# Use Pyhive to query hive
conn = hive.Connection(host="hive-server", port=10000)
cursor = conn.cursor()
TABLE_NAME = "ktk"

# TODO: test partitioned dataset
for filepath in store.iter_keys():
    if filepath.endswith(".parquet"):
        parquet_file_parentdir = f"{VOLUME_LOCATION}/{os.path.dirname(filepath)}"

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
# Pyarrow stores timestamp as microseconds from epoch, convert to date
hive_df["datetime64"] = pd.to_datetime(hive_df.loc[:, "datetime64"] * 1000, unit="ns")
# Output from hive is a string, parse this to date
hive_df["date_"] = pd.to_datetime(hive_df.loc[:, "date_"], format="%Y-%m-%d").apply(
    lambda x: x.date()
)

# Ignore dtype for numeric comparisons (e.g. int32 with int64)
pdt.assert_frame_equal(df[selected_columns], hive_df, check_dtype=False)
print(f"Test completed for the following data types: {selected_columns}")
