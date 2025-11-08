import logging
from src.config import create_spark_session

logger = logging.getLogger(__name__)

class GoldLayerAnalytics:
    def __init__(self):
        self.spark = create_spark_session()
        logger.info("Initialized GoldLayerAnalytics with Spark session")