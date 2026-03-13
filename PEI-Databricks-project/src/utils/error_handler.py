
from pyspark.sql.functions import col
from src.utils.logger import logger

def validate_dataframe(df):

    if df.rdd.isEmpty():
        raise ValueError("Dataset empty")

    corrupt = df.filter(col("_corrupt_record").isNotNull())

    if corrupt.count() > 0:
        logger.warning("Corrupt records detected")
