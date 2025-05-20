import boto3
from pyspark.sql import SparkSession
from pyspark import SparkConf

class S3Connector:
    """
    Connector to load data from S3 into Spark DataFrames.
    """

    def __init__(self, spark: SparkSession = None, aws_profile: str = None):
        """
        :param spark: an existing SparkSession; if None, a new one will be created.
        :param aws_profile: (optional) name of your AWS CLI profile for boto3.
        """
        if aws_profile:
            boto3.setup_default_session(profile_name=aws_profile)
        self.spark = spark or SparkSession.builder.getOrCreate()

    def load(self, path: str, format: str = "parquet", options: dict = None):
        """
        Load data from S3 into a Spark DataFrame.

        :param path: S3 URI (e.g. "s3://my-bucket/path/")
        :param format: data format ("parquet", "csv", "json", etc.)
        :param options: additional Spark read options, e.g. {"header": "true", "inferSchema": "true"}
        :return: Spark DataFrame
        """
        reader = self.spark.read.format(format)
        if options:
            reader = reader.options(**options)
        return reader.load(path)