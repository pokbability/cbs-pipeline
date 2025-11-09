import logging
from src.config import create_spark_session, Base
import os
import pathlib
from src.constants import *
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, sha2, broadcast,
    year, month, dayofmonth, current_timestamp,trim, date_format
)

logger = logging.getLogger(__name__)

class Load(Base):
    def __init__(self):
        self.spark = create_spark_session()
        logger.info("Initialised loading to curated layer with Spark session")

    
        
    def build_dim_customers(self):
        
        customers = self._read_table(INT+UNDERSCORE+CUSTOMERS, TRANSFORMED_PATH)

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

        self._write_table(dim_customers, DIM + UNDERSCORE + CUSTOMERS, CURATED_PATH)
        return dim_customers

    def build_dim_accounts(self):
        accounts = self._read_table(INT+UNDERSCORE+ACCOUNTS, TRANSFORMED_PATH)

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

        self._write_table(dim_accounts, DIM+UNDERSCORE+ACCOUNTS, CURATED_PATH)
        return dim_accounts

    def build_dim_date(self):
        transactions = self._read_table(INT+UNDERSCORE+TRANSACTIONS, TRANSFORMED_PATH)

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

        self._write_table(dim_date, DIM+DATE, CURATED_PATH)
        return dim_date

    def build_dim_transaction_types(self):
        transactions = self._read_table(INT+UNDERSCORE+TRANSACTIONS, TRANSFORMED_PATH)

        dim_tx_types = (
            transactions
            .select(lower(col(TRANSACTION_TYPE)).alias(TRANSACTION_TYPE))
            .where(col(TRANSACTION_TYPE).isNotNull())
            .dropDuplicates([TRANSACTION_TYPE]) 
            .withColumn(DIM_TRANSACTION_TYPE_ID, sha2(col(TRANSACTION_TYPE), 256))
            .select(DIM_TRANSACTION_TYPE_ID, TRANSACTION_TYPE)
        )

        self._write_table(dim_tx_types, DIM+TRANSACTION_TYPE, CURATED_PATH)
        return dim_tx_types

    
    def build_fact_transactions(self):
        
        tx_prepro = self._read_table(INT+UNDERSCORE+TRANSACTIONS, TRANSFORMED_PATH)

        
        dim_accounts = self._read_table(DIM +UNDERSCORE+ ACCOUNTS, CURATED_PATH)
        dim_customers = self._read_table(DIM +UNDERSCORE+ CUSTOMERS, CURATED_PATH)
        dim_date = self._read_table(DIM +UNDERSCORE+ DATE, CURATED_PATH)
        dim_types = self._read_table(DIM +UNDERSCORE+ TRANSACTION_TYPE, CURATED_PATH)


        accounts_lookup = dim_accounts.select(DIM_ACCOUNT_ID, ACCOUNT_ID, CUSTOMER_ID)
        
        customers_lookup = dim_customers.select(DIM_CUSTOMER_ID, CUSTOMER_ID)
        
        date_lookup = broadcast(dim_date.select(DIM_DATE_ID, DATE))
        type_lookup = broadcast(dim_types.select(DIM_TRANSACTION_TYPE_ID, TRANSACTION_DATE))

        fact_tx = (
            tx_prepro.join(accounts_lookup, on=ACCOUNT_ID, how="left")
            .join(customers_lookup,on=CUSTOMER_ID, how="left")
            .join(date_lookup,tx_prepro.transaction_type == date_lookup.date,"left")
            .join(type_lookup, on=TRANSACTION_TYPE, how="left")
        )


        final_tx = (
            fact_tx
            .withColumn(FACT_TX_ID, sha2(col(TRANSACTION_ID), 256))
            .withColumn(PROCESSED_TIMESTAMP, current_timestamp())
            .select(
                FACT_TX_ID,
                TRANSACTION_ID,
                DIM_TRANSACTION_TYPE_ID,
                DIM_ACCOUNT_ID,
                DIM_CUSTOMER_ID,
                DIM_DATE_ID,
                AMOUNT,
                DESCRIPTION,
                TRANSACTION_DATE,
                PROCESSED_TIMESTAMP
            )
        )

        self._write_partitioned(final_tx, FACT+TRANSACTIONS, [TRANSACTION_DATE])
        return final_tx

    
    def create_star_schema(self):
        logger.info("Starting star schema creation")
        
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")  # Enable AQE
        

        logger.info("Building and caching dimension tables")
            
        dim_customers = self.build_dim_customers()
        dim_customers.cache()
            
        dim_accounts = self.build_dim_accounts()
        dim_accounts.cache()
            
        dim_date = self.build_dim_date()
        dim_date.cache()
            
        dim_types = self.build_dim_transaction_types()
        dim_types.cache() 
            

            
        logger.info("Building fact table")
        self.build_fact_transactions()
            
        # Clear caches after fact table is built
        self.spark.catalog.clearCache()
            