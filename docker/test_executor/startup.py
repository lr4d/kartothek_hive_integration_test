#!/usr/bin/env python

import os
from functools import partial

import pandas as pd
import pandas.testing as pdt
import storefact
from kartothek.io.eager import store_dataframes_as_dataset, read_table
from kartothek.serialization.testing import get_dataframe_not_nested
from pyhive import hive

VOLUME_LOCATION = "/parquet_data"


def _create_hive_table(
    cursor,
    table_name,
    selected_columns_and_dtypes,
    partition_on,
    parquet_file_parentdir,
):
    partitioned_by_arg = None
    if partition_on:
        for _ in partition_on:
            # Get the parent directory of the parquet file for each column it is partitioned on
            # Note: Parquet filepath looks like: `/tmp/uuid/table/partition_col1=x/partition_col2=y/1300dadda3.parquet`
            parquet_file_parentdir = os.path.dirname(parquet_file_parentdir)

        partitioned_by_arg = f"""PARTITIONED BY ({
        ", ".join([f"{col_name} {selected_columns_and_dtypes.pop(col_name, 'STRING')}" for col_name in partition_on])
        })"""

    # Hive allows us to only select a subset of columns to be loaded from the Parquet file
    columns_arg = ",\n                ".join(
        [
            f"{col_name} {dtype}"
            for col_name, dtype in selected_columns_and_dtypes.items()
        ]
    )
    hive_query = f"""
          CREATE external table {table_name} (
               {columns_arg}
            )
            {partitioned_by_arg if partitioned_by_arg else ""}
          STORED AS PARQUET
          LOCATION "{parquet_file_parentdir}"
        """

    print(f"Hive query: {hive_query}")
    cursor.execute(hive_query)


def _assert_hive_frame_equal(hive_df, expected_df, expected_columns):

    # Pyarrow stores timestamp as microseconds from epoch, convert to date
    hive_df["datetime64"] = pd.to_datetime(
        hive_df.loc[:, "datetime64"] * 1000, unit="ns"
    )
    # Output from hive is a string, parse this to date
    hive_df["date_"] = pd.to_datetime(hive_df.loc[:, "date_"], format="%Y-%m-%d").apply(
        lambda x: x.date()
    )

    hive_df = hive_df.sort_values("bytes").reset_index(drop=True)
    expected_df = expected_df.sort_values("bytes").reset_index(drop=True)[
        expected_columns
    ]
    # Ignore dtype for numeric comparisons (e.g. int32 with int64)
    pdt.assert_frame_equal(expected_df, hive_df, check_dtype=False)
    assert len(hive_df) > 0


def assert_hive_compat(
    dfs, store_factory, uuid, partition_on=None, **store_dataframes_kwargs
):
    TABLE_NAME = uuid  # Hive table name

    store_dataframes_as_dataset(
        store=store_factory,
        dataset_uuid=uuid,
        dfs=dfs,
        partition_on=partition_on,
        **store_dataframes_kwargs,
    )
    expected_df = read_table(
        store=store_factory, dataset_uuid=uuid, dates_as_object=True
    )
    expected_columns = list(expected_df.columns)

    print(f"Dataset location: {VOLUME_LOCATION}")

    # Use Pyhive to query hive
    conn = hive.Connection(host="hive-server", port=10000)
    cursor = conn.cursor()

    # Create Hive table
    ## Non-nested columns not included: `np.uint64` (max value is too large for `BIGINT`)
    ## The `null` column can be specified as multiple types (at least `STRING` and `FLOAT`)
    # TODO: have a mapping from kartothek/arrow dtypes to Hive dtypes
    selected_columns_and_dtypes = {
        "bool": "BOOLEAN",
        "bytes": "BINARY",
        "date_": "DATE",
        "datetime64": "BIGINT",
        "float32": "FLOAT",
        "float64": "DOUBLE",
        "int8": "TINYINT",
        "int16": "SMALLINT",
        "int32": "INT",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INT",
        "uint32": "BIGINT",
        "unicode": "STRING",
        "null_": "FLOAT",
    }

    for filepath in store_factory().iter_keys():
        if filepath.endswith(".parquet"):
            parquet_file_parentdir = f"{VOLUME_LOCATION}/{os.path.dirname(filepath)}"
            break

    print(
        _create_hive_table(
            cursor,
            TABLE_NAME,
            selected_columns_and_dtypes,
            partition_on,
            parquet_file_parentdir,
        )
    )

    if partition_on:
        # If on Hive >= 4.0, this code block should be removed and the following should be used:
        # https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-DiscoverPartitions
        cursor.execute(f"MSCK REPAIR TABLE {TABLE_NAME}")
        for partition_column in partition_on:
            if partition_column in expected_columns:
                expected_columns.remove(partition_column)

    expected_columns = list(selected_columns_and_dtypes.keys()) + partition_on
    # Read hive table into pandas
    hive_df = pd.read_sql(
        f"SELECT {', '.join(expected_columns)} FROM {TABLE_NAME}", conn
    )
    hive_df.columns = [col.replace(f"{TABLE_NAME}.", "") for col in hive_df.columns]

    _assert_hive_frame_equal(hive_df, expected_df, expected_columns)

    print(
        f"Test completed for the following data types: {[sc.rstrip('_') for sc in expected_columns]}"
    )


if __name__ == "__main__":
    # Create dataset on local filesystem
    store_factory = partial(storefact.get_store_from_url, f"hfs://{VOLUME_LOCATION}")

    df = get_dataframe_not_nested(100)
    df["partition_col"] = df.date.apply(str)

    # Rename because `date` and `null` are reserved in Hive QL
    df = df.rename(columns={"date": "date_", "null": "null_"})

    dfs = [df, df]
    assert_hive_compat(dfs, store_factory, uuid="test", partition_on=["partition_col"])
