import pathlib
import os
import json
import glob
from src.constants import *
from pyspark.sql.functions import current_timestamp, input_file_name
from src.config import create_spark_session, Base, setup_logging
from src.schema import IngestionSchema

setup_logging(log_level="INFO", log_file=LOGS+SLASH+INGESTION+DOT+LOG)

import logging
logger = logging.getLogger(__name__)



class Ingestion(Base):
    def __init__(self):
        self.spark = create_spark_session(INGESTION)
        
        file_location = pathlib.Path(__file__).parent.resolve()
        self.raw_root = os.path.realpath(os.path.join(file_location,
                                                        DOTDOT,
                                                        RAW_PATH))
        
        self.watermark_file = os.path.realpath(os.path.join(file_location,
                                                        DOTDOT,
                                                        CONTROL,
                                                        WATERMARK_FILE))
        os.makedirs(os.path.dirname(self.watermark_file), exist_ok=True)
        logger.info("Initialise Ingestion Spark Session")

    def load_watermark(self):
        if not os.path.exists(self.watermark_file):
            return {}
        try:
            with open(self.watermark_file, 'r') as f:
                return json.load(f)
        except:
            logger.warning("Corrupted watermark file")
            return {}


    def save_watermark(self, watermark_dict):
        with open(self.watermark_file, 'w') as f:
            json.dump(watermark_dict, f, indent=2)

    def get_new_files(self, file_pattern, table_name):
        watermark = self.load_watermark()
        last_updated_file = watermark.get(table_name)

        all_files = sorted(glob.glob(os.path.join(self.raw_root, file_pattern)))

        if not all_files:
            logger.info(f"No files found for pattern {file_pattern}")
            return

        if not last_updated_file:
            logger.info(f"No previous watermark for {file_pattern}, ingesting all files")
            new_files = all_files
        else:
            new_files = [f for f in all_files if os.path.basename(f) > last_updated_file]
            logger.info(f"Last processed: {last_updated_file} | Found {len(new_files)} new file(s) for {table_name}")

        return new_files
    
    def ingestion_file(self,  file_pattern, table_name, schema, partition_cols=None):
        
        new_files = self.get_new_files(file_pattern, table_name)

        if not new_files:
            logger.info(f"No new files for {table_name}. Skipping.")
            return
        file_location = pathlib.Path(__file__).parent.resolve()
        
        df = (self.spark.read
                  .format("csv")
                  .option("header", "true")
                  .schema(schema)
                  .load(new_files))
        
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        staging_path = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    STAGING_PATH,
                                                    table_name))
        

        writer = (df.write
              .format("delta")
              .mode("append")
              .option("mergeSchema", "true"))
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
       
        writer.save(staging_path)
        
    
        latest_file = os.path.basename(sorted(new_files)[-1])
        watermark = self.load_watermark()
        watermark[table_name] = latest_file
        self.save_watermark(watermark)

        logger.info(f"Successfully ingested {table_name} | Latest file: {latest_file}")
    
    def ingest_all(self):
        try:
            logger.info("Starting full ingestion process")
            
            self.ingestion_file(
                ACCOUNTS+STAR+DOT+CSV, 
                STG+UNDERSCORE+ACCOUNTS, 
                IngestionSchema.accounts_fields()
            )
            self.ingestion_file(
                CUSTOMERS+STAR+DOT+CSV, 
                STG+UNDERSCORE+CUSTOMERS, 
                IngestionSchema.customers_fields()
            )
            self.ingestion_file(
                TRANSACTIONS+STAR+DOT+CSV, 
                STG+UNDERSCORE+TRANSACTIONS, 
                IngestionSchema.transactions_fields(),
                partition_cols=[TRANSACTION_DATE]
            )
        finally:
            self.spark.stop()