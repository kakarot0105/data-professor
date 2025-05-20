import os
import pytest
from pyspark.sql import SparkSession
from data_professor.connectors.s3 import S3Connector

@pytest.fixture(scope="module")
def spark():
    """
    Create a SparkSession for tests.
    """
    return SparkSession.builder \
        .master("local[*]") \
        .appName("data_professor_test") \
        .getOrCreate()

def test_s3connector_load_parquet(tmp_path, spark):
    """
    Write a small DataFrame to Parquet under tmp_path and
    verify that S3Connector.load can read it back correctly.
    """
    # 1) Create sample DataFrame
    data = [(1, "foo"), (2, "bar"), (3, None)]
    df_original = spark.createDataFrame(data, ["id", "value"])

    # 2) Write to Parquet
    out_dir = tmp_path / "parquet_data"
    df_original.write.parquet(str(out_dir))

    # 3) Load via connector
    connector = S3Connector(spark=spark)
    df_loaded = connector.load(str(out_dir), format="parquet")

    # 4) Verify schema & row count
    assert set(df_loaded.columns) == set(df_original.columns)
    assert df_loaded.count() == df_original.count()

    # 5) Verify content equivalence
    orig_set = {tuple(row) for row in df_original.collect()}
    loaded_set = {tuple(row) for row in df_loaded.collect()}
    assert orig_set == loaded_set

def test_s3connector_load_csv(tmp_path, spark):
    """
    Write a small DataFrame to CSV and verify loading with options.
    """
    data = [("alice", 30), ("bob", None)]
    df_original = spark.createDataFrame(data, ["name", "age"])

    # Write CSV with header
    out_dir = tmp_path / "csv_data"
    df_original.write.option("header", "true").csv(str(out_dir))

    # Load via connector with CSV options
    connector = S3Connector(spark=spark)
    df_loaded = connector.load(
        path=str(out_dir),
        format="csv",
        options={"header": "true", "inferSchema": "true"}
    )

    # Schema should match
    assert set(df_loaded.columns) == set(df_original.columns)
    # Row count should match
    assert df_loaded.count() == df_original.count()

    # Check that None/NULL was preserved
    loaded_data = {tuple(row) for row in df_loaded.collect()}
    orig_data = {tuple(row) for row in df_original.collect()}
    assert loaded_data == orig_data