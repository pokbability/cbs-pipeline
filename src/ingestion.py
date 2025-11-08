import logging
from src.config import create_spark_session

logger = logging.getLogger(__name__)

class Ingestion:
    def __init__(self):
        self.spark = create_spark_session("ingestion")
        logger.info("Initialise Ingestion Spark Session")

    def get_struct(self):
        pass

    
    def ingestion(self):
        pass