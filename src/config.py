from pyspark.sql import SparkSession
import os
import pathlib
from src.constants import *


def create_spark_session(session_name):

    return (SparkSession.builder
            .appName(session_name)
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
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
    
    def _write_table(self, df, table_name, path):
        file_location = pathlib.Path(__file__).parent.resolve()
        table = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    path,
                                                    table_name))
        df.write.format("delta") \
            .mode("overwrite") \
            .save(table)
        
    def _write_partitioned(self, df, table_name, path, partition_cols):

        file_location = pathlib.Path(__file__).parent.resolve()
        table = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    path,
                                                    table_name))
        writer = df.write.format("delta").mode("overwrite")
            
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            
        writer = writer.option("dataChange", "true") \
                         .option("optimizeWrite", "true")
            
        writer.save(table)


