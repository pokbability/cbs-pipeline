import logging
import pathlib
import os
from src.constants import *
from pyspark.sql.functions import current_timestamp, input_file_name
from src.config import create_spark_session
from src.schema import IngestionSchema

logger = logging.getLogger(__name__)

class Ingestion:
    def __init__(self):
        self.spark = create_spark_session(INGESTION)
        logger.info("Initialise Ingestion Spark Session")

    
    def ingestion_file(self, source_file, table_name, schema):
        

        logger.info(f"Starting ingestion for {source_file} into staging {table_name} table")
        file_location = pathlib.Path(__file__).parent.resolve()
        raw_file_path = os.path.realpath(os.path.join(file_location,
                                                      DOTDOT,
                                                      RAW_PATH,
                                                      source_file))
        
        df = (self.spark.read
                  .format("csv")
                  .option("header", "true")
                  .schema(schema)
                  .load(raw_file_path))
        
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        staging_path = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    STAGING_PATH,
                                                    table_name))
        

        df.write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .save(staging_path)
        
    
        logger.info(f"Completed ingestion for {source_file} into staging {table_name} table")
    
    def ingest_all(self):
        self.ingestion_file(
            ACCOUNTS+DOT+CSV, 
            STG+UNDERSCORE+ACCOUNTS, 
            IngestionSchema.accounts_fields()
        )
        self.ingestion_file(
            CUSTOMERS+DOT+CSV, 
            STG+UNDERSCORE+CUSTOMERS, 
            IngestionSchema.customers_fields()
        )
        self.ingestion_file(
            TRANSACTIONS+DOT+CSV, 
            STG+UNDERSCORE+TRANSACTIONS, 
            IngestionSchema.transactions_fields()
        )