import pytest

import os
import shutil
from copy import deepcopy
from functools import partial

import storefact
import pandas as pd
from pyhive import hive
import pandas.testing as pdt
from kartothek.serialization.testing import get_dataframe_not_nested
from kartothek.io.eager import store_dataframes_as_dataset, read_table


#### Fixtures ####
@pytest.fixture()
def test_df():
    df = _create_df()
    # Rename because `date` and `null` are reserved in Hive QL
    df = df.rename(columns={"date": "date_", "null": "null_"})
    return df


@pytest.fixture()
def volume_location(request):
    volume_location = "/parquet_data"

    def cleanup_data():

        for item in os.listdir(volume_location):
            itempath = os.path.join(volume_location, item)
            if os.path.isdir(itempath):
                shutil.rmtree(itempath)
            else:
                os.remove(itempath)

    yield volume_location
    request.addfinalizer(cleanup_data)


@pytest.fixture()
def store_factory(volume_location):
    store_factory = partial(storefact.get_store_from_url, f"hfs://{volume_location}")
    return store_factory


@pytest.fixture(scope="session")
def uuid():
    return "test"


@pytest.fixture(scope="session")
def tested_columns_and_dtypes():
    ## Non-nested columns not included: `np.uint64` (max value is too large for `BIGINT`)
    ## The `null` column can be specified as multiple types (at least `STRING` and `FLOAT`)
    # TODO: have a mapping from kartothek/arrow dtypes to Hive dtypes
    tested_columns_and_dtypes = {
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
    return tested_columns_and_dtypes


@pytest.fixture(
    params=[
        None,
        {"date_": {"cast_as": str, "hive_type": "STRING"}},
        {"date_": {"cast_as": str, "hive_type": "STRING"}, "int32": None},
    ]
)
def partition_on(request, tested_columns_and_dtypes):
    partition_on = request.param and {
        f"partition_{col_idx}": {
            "source_col": source_col,
            **(
                col_info
                or {"hive_type": tested_columns_and_dtypes[source_col], "cast_as": None}
            ),
        }
        for col_idx, (source_col, col_info) in enumerate(request.param.items())
    }
    return partition_on


#### Test function ####
def test_hive_compat(
    test_df,
    volume_location,
    store_factory,
    uuid,
    tested_columns_and_dtypes,
    partition_on,
):

    df = _create_df(test_df, partition_on)
    hive_df, expected_df, expected_columns = _prep_data(
        df=df,
        volume_location=volume_location,
        store_factory=store_factory,
        uuid=uuid,
        tested_columns_and_dtypes=tested_columns_and_dtypes,
        partition_on=partition_on,
    )

    _assert_hive_frame_equal(hive_df, expected_df, expected_columns)
    _print_success_msg(expected_columns, partition_on)


#### Helper functions ####
def _create_df(df=None, partition_cols=None):

    # Create dataset on local filesystem
    if df is None:
        df = get_dataframe_not_nested(100)
    if partition_cols:
        for partition_col, partition_from in partition_cols.items():
            source_col = partition_from["source_col"]
            cast_as = partition_from["cast_as"]
            # create partitioning column and cast if require
            df[partition_col] = df[source_col].apply(
                lambda x: cast_as and cast_as(x) or x
            )
    return df


def _prep_data(
    df,
    volume_location,
    store_factory,
    uuid,
    tested_columns_and_dtypes,
    partition_on=None,
):

    dfs = [df, df]
    TABLE_NAME = uuid  # Hive table name

    store_dataframes_as_dataset(
        store=store_factory,
        dataset_uuid=uuid,
        dfs=dfs,
        partition_on=partition_on and list(partition_on),
    )
    expected_df = read_table(
        store=store_factory, dataset_uuid=uuid, dates_as_object=True
    )
    print(f"Dataset location: {volume_location}")

    conn = hive.Connection(host="hive-server", port=10000)
    cursor = conn.cursor()

    parquet_file_parentdir = None
    for filepath in store_factory().iter_keys():
        if filepath.endswith(".parquet"):
            parquet_file_parentdir = (
                f"{volume_location}{os.path.sep}{os.path.dirname(filepath)}"
            )
            break

    # Create Hive table
    selected_columns_and_dtypes = deepcopy(tested_columns_and_dtypes)
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
        selected_columns_and_dtypes.update(partition_on)

    expected_columns = list(selected_columns_and_dtypes)  # + partition_on

    # Read hive table into pandas
    hive_df = pd.read_sql(
        f"SELECT {', '.join(expected_columns)} FROM {TABLE_NAME}", conn
    )

    return hive_df, expected_df, expected_columns


def _create_hive_table(
    cursor,
    table_name,
    selected_columns_and_dtypes,
    partition_on,
    parquet_file_parentdir,
):

    cursor.execute(f"drop table if exists {table_name}")

    partitioned_by_arg = None
    if partition_on:
        partitioned_by_arg = f"""PARTITIONED BY ({
        ", ".join([f"{col_name} {col_info['hive_type']}" for col_name, col_info in partition_on.items()])
        })"""

        dataset_parentdir = _get_dataset_file_path(parquet_file_parentdir, list(partition_on))
    else:
        dataset_parentdir = parquet_file_parentdir

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
            {partitioned_by_arg or ""}
          STORED AS PARQUET
          LOCATION "{dataset_parentdir}"
        """
    print(f"Hive query: {hive_query}")
    cursor.execute(hive_query)

    return "Table created"


def _get_dataset_file_path(parquet_file_parentdir, partition_on):
    # Get the parent directory of the dataset
    # Note: Parquet filepath looks like: `/tmp/uuid/table/partition_col1=x/partition_col2=y`
    # from which we want : `/tmp/uuid/table`
    # we do this by finding the index of the column names in the path string
    # and returning the path upto the minimum index
    required_path = min(
        parquet_file_parentdir[: parquet_file_parentdir.index(col_name) - 1]
        for col_name in list(partition_on)
    )
    return required_path


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


def _print_success_msg(expected_columns, partition_on=None):

    msg_if_partitioned = (
        "".join(
            (
                " partitioned on ",
                " and ".join(
                    f"{col_name} ({col_info['source_col'].rstrip('_')} as {col_info['hive_type']})"
                    for col_name, col_info in partition_on.items()
                ),
                " ",
            )
        )
        if partition_on
        else " "
    )
    cols_to_print = [
        ec.rstrip("_")
        for ec in expected_columns
        if ec not in (partition_on and list(partition_on) or [])
    ]
    print(
        f"Test completed successfully on test dataset{msg_if_partitioned}for the following data types: {cols_to_print}"
    )
