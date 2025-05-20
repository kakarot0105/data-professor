import os
import yaml
from pyspark.sql import SparkSession

from data_professor.connectors.s3 import S3Connector
from data_professor.profiler import Profiler
from data_professor.qa import QAEngine

class DataProfessor:
    """
    Orchestrates loading data, profiling, and answering questions via LLM.
    """

    def __init__(self, config_path: str = "config/default.yaml"):
        # Load YAML config
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Initialize Spark
        spark_conf = SparkSession.builder \
            .master(self.config["spark"]["master"]) \
            .appName("DataProfessor")
        self.spark = spark_conf.getOrCreate()

        # AWS credentials (if set via env or profile)
        aws_profile = os.getenv("AWS_PROFILE")
        self.connector = S3Connector(spark=self.spark, aws_profile=aws_profile)

        # Profiler and QA engine
        self.profiler = Profiler()
        self.qa_engine = QAEngine(llm_config=self.config["llm"])

        # Placeholders
        self.df = None
        self.metadata = None

    def load_source(self, source: str = None, format: str = None, options: dict = None):
        """
        Load data into a Spark DataFrame and profile it.

        :param source: override source path (e.g. s3://...)
        :param format: override format (parquet, delta, csv, etc.)
        :param options: Spark read options dict
        """
        path = source or self.config["source"]["path"]
        fmt = format or self.config["source"]["format"]
        self.df = self.connector.load(path, fmt, options=options)
        self.metadata = self.profiler.profile(self.df)
        return self.df

    def profile(self):
        """
        Re-run profiling on the current DataFrame.
        """
        if self.df is None:
            raise RuntimeError("No DataFrame loaded. Call load_source() first.")
        self.metadata = self.profiler.profile(self.df)
        return self.metadata

    def ask(self, question: str) -> str:
        """
        Ask a natural-language question about the loaded dataset.

        :param question: free-form query string
        :return: answer text
        """
        if self.metadata is None:
            raise RuntimeError("No metadata available. Call load_source() or profile() first.")
        return self.qa_engine.answer(question, metadata=self.metadata)