import os
import pathlib
import hashlib
import hmac
from src.config import create_spark_session

def _mask_generic_str(val):
    if val is None:
        return None
    s = str(val)
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]



def _hash_string(val: str):
    if val is None:
        return None
    salt = (os.getenv("PII_SALT") or "dev_salt_for_local_testing").encode("utf-8")
    return hmac.new(salt, str(val).encode("utf-8"), hashlib.sha256).hexdigest()


def read_delta_table(table_name, path):
    spark = create_spark_session("ReadDeltaTable")
    file_location = pathlib.Path(__file__).parent.resolve()
    file_path = os.path.realpath(os.path.join(file_location,
                                                  "..",
                                                  path,
                                                  table_name))
    df = spark.read.format("delta").load(file_path)

    print(f"\nSchema of Delta table:")
    df.printSchema()
    
    print(f"\nNumber of records: {df.count()}")
    
    print(f"\nFirst 10 rows:")
    df.show(10, truncate=False)
    spark.stop()
