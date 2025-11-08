import logging
from src.config import create_spark_session
import os
import pathlib
from src.constants import *
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, sha2,
    year, month, dayofmonth, current_timestamp,trim, date_format
)

logger = logging.getLogger(__name__)

class Load:
    def __init__(self):
        self.spark = create_spark_session()
        logger.info("Initialised loading to curated layer with Spark session")

    def _read_transformed_table(self, table_name):
        file_location = pathlib.Path(__file__).parent.resolve()
        transformed_table = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    TRANSFORMED_PATH,
                                                    table_name))
        
        return self.spark.read.format("delta").load(transformed_table)
    
    def _write_curated_table(self, df, table_name):
        file_location = pathlib.Path(__file__).parent.resolve()
        curated_table = os.path.realpath(os.path.join(file_location,
                                                    DOTDOT,
                                                    CURATED_PATH,
                                                    table_name))
        df.write.format("delta") \
            .mode("overwrite") \
            .save(curated_table)
        
    def build_dim_customers(self):
        
        customers = self._read_transformed_table(INT+UNDERSCORE+CUSTOMERS)

        dim_customers = (
            customers
            .select(
                col(CUSTOMER_ID),
                col(FIRST_NAME),
                col(LAST_NAME),
                col(DATE_OF_BIRTH),
                col(CITY),
                col(STATE),
                col(ZIPCODE),
                col(PROCESSED_TIMESTAMP)
            )
            .dropDuplicates([CUSTOMER_ID])
            .withColumn(DIM_CUSTOMER_ID, sha2(col(CUSTOMER_ID), 256))
            .select(
                DIM_CUSTOMER_ID,
                CUSTOMER_ID,
                FIRST_NAME,
                LAST_NAME,
                DATE_OF_BIRTH,
                CITY,
                STATE,
                ZIPCODE,
                PROCESSED_TIMESTAMP
            )
        )

        self._write_curated_table(dim_customers, DIM + UNDERSCORE + CUSTOMERS)
        return dim_customers

    def build_dim_accounts(self):
        accounts = self._read_transformed_table(INT+UNDERSCORE+ACCOUNTS)

        dim_accounts = (
            accounts
            .select(
                col(ACCOUNT_ID),
                col(CUSTOMER_ID),
                col(ACCOUNT_TYPE),
                col(OPENING_DATE),
                col(BALANCE),
                col(PROCESSED_TIMESTAMP)
            )
            .dropDuplicates([ACCOUNT_ID])
            .withColumn(DIM_ACCOUNT_ID, sha2(col(ACCOUNT_ID), 256))
            .select(
                DIM_ACCOUNT_ID,
                ACCOUNT_ID,
                CUSTOMER_ID,
                ACCOUNT_TYPE,
                OPENING_DATE,
                BALANCE,
                PROCESSED_TIMESTAMP
            )
        )

        self._write_curated_table(dim_accounts, DIM+UNDERSCORE+ACCOUNTS)
        return dim_accounts

    def build_dim_date(self):
        transactions = self._read_transformed_table(INT+UNDERSCORE+TRANSACTIONS)

        dim_date = (
            transactions
            .select(col(TRANSACTION_DATE))
            .where(col(TRANSACTION_DATE).isNotNull())
            .dropDuplicates([TRANSACTION_DATE]) 
            .withColumn(DIM_DATE_ID, date_format(col(TRANSACTION_DATE), "yyyyMMdd"))
            .withColumn(YEAR, year(col(TRANSACTION_DATE)))
            .withColumn(MONTH, month(col(TRANSACTION_DATE)))
            .withColumn(DAY, dayofmonth(col(TRANSACTION_DATE)))
            .select(
                DIM_DATE_ID,
                col(TRANSACTION_DATE).alias(DATE),
                YEAR,
                MONTH,
                DAY
            )
        )

        self._write_curated_table(dim_date, DIM+DATE)
        return dim_date

    def build_dim_transaction_types(self):
        transactions = self._read_transformed_table(INT+UNDERSCORE+TRANSACTIONS)

        dim_tx_types = (
            transactions
            .select(lower(col(TRANSACTION_TYPE)).alias(TRANSACTION_TYPE))
            .where(col(TRANSACTION_TYPE).isNotNull())
            .dropDuplicates([TRANSACTION_TYPE]) 
            .withColumn(DIM_TRANSACTION_TYPE_ID, sha2(col(TRANSACTION_TYPE), 256))
            .select(DIM_TRANSACTION_TYPE_ID, TRANSACTION_TYPE)
        )

        self._write_curated_table(dim_tx_types, DIM+TRANSACTION_TYPE)
        return dim_tx_types

    
        
    