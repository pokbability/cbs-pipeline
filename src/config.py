from pyspark.sql import SparkSession
import os
import pathlib
from src.constants import *
import logging
import sys
from pathlib import Path


def create_spark_session(session_name):

    return (SparkSession.builder
            .appName(session_name)
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

class Base:
    def __init__(self):
        self.spark = create_spark_session()

    def _read_table(self, table_name, path):
        file_location = pathlib.Path(__file__).parent.resolve()
        table = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    path,
                                                    table_name))
        return self.spark.read.format("delta").load(table)
    
    def _write_table(self, df, table_name, path, partition_cols=None):
        file_location = pathlib.Path(__file__).parent.resolve()
        table = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    path,
                                                    table_name))
        writer = (
            df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            writer = writer.option("dataChange", "true") \
                         .option("optimizeWrite", "true")
        
        writer.save(table)


def setup_logging(log_level= "WARN", log_file=None):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.handlers.clear()

    level = getattr(logging, log_level.upper())


    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)


    handlers = [console_handler]
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        handlers.append(file_handler)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)-30s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%H:%M:%S"
    )

    for handler in handlers:
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    root_logger.setLevel(level)

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)

    logging.info("Logging initialized → level=%s", log_level.upper())
        

