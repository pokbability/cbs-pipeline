from pyspark.sql import SparkSession


def create_spark_session(session_name):

    return (SparkSession.builder
            .appName(session_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

