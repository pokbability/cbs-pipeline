import logging
from src.config import create_spark_session, Base
from src.constants import *
import pathlib
import os
from src.functions import _hash_string, _mask_generic_str
from pyspark.sql.functions import (
    col, coalesce, trim, lit, lower, regexp_replace, to_date, udf, when, to_date, current_timestamp
)
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

class Transformation(Base):
    def __init__(self):
        self.spark = create_spark_session(TRANSFORMATION)
        logger.info("Initialised Transformation Spark Session")

        self._pii_strategy = os.getenv("PII_STRATEGY", "hash").lower()
        self._pii_salt = os.getenv("PII_SALT")
        if self._pii_strategy == "hash" and not self._pii_salt:
            logger.warning("PII_STRATEGY=hash selected but PII_SALT is not set; using a default dev salt")

        self._hash_udf = udf(_hash_string, StringType())
        self._mask_generic_udf = udf(_mask_generic_str, StringType())


    def transform_customers(self):
        logger.info("Cleansing customer data")
        stg_customers_table = self._read_table(STG+UNDERSCORE+CUSTOMERS, STAGING_PATH)

        if self._pii_strategy == "hash":
            int_customers_df = stg_customers_table.select(
                trim(col(CUSTOMER_ID)).alias(CUSTOMER_ID),
                self._hash_udf(trim(col(FIRST_NAME))).alias(FIRST_NAME),
                self._hash_udf(trim(col(LAST_NAME))).alias(LAST_NAME),
                to_date(col(DATE_OF_BIRTH), "yyyy-MM-dd").alias(DATE_OF_BIRTH),
                self._mask_generic_udf(trim(col(ADDRESS))).alias(ADDRESS),
                trim(col(CITY)).alias(CITY),
                trim(col(STATE)).alias(STATE),
                regexp_replace(trim(col(ZIP)), "[^0-9]", "").alias(ZIPCODE)
            ).dropDuplicates([CUSTOMER_ID])
        else:
            int_customers_df = stg_customers_table.select(
                trim(col(CUSTOMER_ID)).alias(CUSTOMER_ID),
                self._hash_udf(trim(col(FIRST_NAME))).alias(FIRST_NAME),
                self._hash_udf(trim(col(LAST_NAME))).alias(LAST_NAME),
                to_date(col(DATE_OF_BIRTH), "yyyy-MM-dd").alias(DATE_OF_BIRTH),
                self._mask_generic_udf(trim(col(ADDRESS))).alias(ADDRESS),
                trim(col(CITY)).alias(CITY),
                trim(col(STATE)).alias(STATE),
                regexp_replace(trim(col(ZIP)), "[^0-9]", "").alias(ZIPCODE)
            ).dropDuplicates([CUSTOMER_ID])
        
        self._write_table(int_customers_df, INT+UNDERSCORE+CUSTOMERS, TRANSFORMED_PATH)
        logger.info("Completed cleansing customer data")

    
    def transform_accounts(self):

        logger.info("Cleansing accounts data")
        stg_accounts_table = self._read_table(STG+UNDERSCORE+ACCOUNTS, STAGING_PATH)

        int_accounts_df = stg_accounts_table.select(
            trim(col(ACCOUNT_ID)).alias(ACCOUNT_ID),
            trim(col(CUSTOMER_ID)).alias(CUSTOMER_ID),              
            lower(trim(col(ACCOUNT_TYPE))).alias(ACCOUNT_TYPE),               
            to_date(col(OPENING_DATE), "yyyy-MM-dd").alias(OPENING_DATE),               
            col(BALANCE).alias(BALANCE),               
            current_timestamp().alias(PROCESSED_TIMESTAMP)
        )
        

        int_accounts_df = int_accounts_df.filter(
            col(ACCOUNT_ID).isNotNull() &
            col(CUSTOMER_ID).isNotNull() &
            col(OPENING_DATE).isNotNull() &
            col(BALANCE).isNotNull() &
            col(ACCOUNT_TYPE).isin("savings", "checking", "loan", "investment")
        )
        

        self._write_table(int_accounts_df, INT+UNDERSCORE+ACCOUNTS, TRANSFORMED_PATH)   
        logger.info("Completed account data transformation")

    def transform_transactions(self):
        logger.info("Cleansing transactions data")
        stg_transactions_table = self._read_table(STG+UNDERSCORE+TRANSACTIONS, STAGING_PATH)
        int_transactions_table = stg_transactions_table.select(
            col(TRANSACTION_ID),
            col(ACCOUNT_ID),
            lower(col(TRANSACTION_TYPE)).alias(TRANSACTION_TYPE),

            when(col(AMOUNT).isNull(), lit(0.0)) \
            .otherwise(col(AMOUNT)).alias(AMOUNT),

            coalesce(
                to_date(col(TRANSACTION_DATE), 'yyyy-MM-dd'),
                current_timestamp()
            ).alias(TRANSACTION_DATE),
            current_timestamp().alias(PROCESSED_TIMESTAMP)
            
        ).dropDuplicates([TRANSACTION_ID])
        

        self._write_table(int_transactions_table, INT+UNDERSCORE+ACCOUNTS, TRANSFORMED_PATH)

    def transform_all(self):
        self.transform_customers()
        self.transform_accounts()
        self.transform_transactions()
        logger.info("Completed transformation layer")

    
            
        
