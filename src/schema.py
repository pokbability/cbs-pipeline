from pyspark.sql.types import StructField, StringType, DecimalType, StructType, DateType
from constants import *

class IngestionSchema:

    @staticmethod
    def account_fields():

        return StructType[
            StructField(ACCOUNT_ID, StringType()),
            StructField(ACCOUNT_TYPE, StringType()),
            StructField(ACCOUNT_TYPE, StringType()),
            StructField(OPENING_DATE, DateType()),
            StructField(BALANCE, DecimalType(38,22))
        ]


    
    @staticmethod
    def customers_fields():
        return StructType[
            StructField(CUSTOMER_ID, StringType()),
            StructField(FIRST_NAME, StringType()),
            StructField(LAST_NAME, StringType()),
            StructField(DATE_OF_BIRTH, DateType()),
            StructField(ADDRESS, DecimalType(38,22)),
            StructField(CITY, StringType()),
            StructField(STATE, DateType()),
            StructField(ZIP, DecimalType(38,22))
        ]
    
    @staticmethod
    def transactions_fields():
        return StructType[
            StructField(TRANSACTION_ID, StringType()),
            StructField(ACCOUNT_ID, StringType()),
            StructField(TRANSACTION_DATE, DateType()),
            StructField(TRANSACTION_TYPE, StringType()),
            StructField(AMOUNT, DecimalType(38,22)),
            StructField(DESCRIPTION, StringType()),
           
        ]