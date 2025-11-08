import logging
from src.config import create_spark_session

logger = logging.getLogger(__name__)

class Transformation:
    def __init__(self):
        self.spark = create_spark_session("ingestion")
        logger.info("Initialised Transformation Spark Session")

    def transform(self):
        pass
    