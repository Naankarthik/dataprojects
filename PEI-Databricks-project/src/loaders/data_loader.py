
from pyspark.sql import SparkSession
from src.utils.error_handler import validate_dataframe
from src.utils.logger import logger

spark = SparkSession.builder.getOrCreate()

def load_data(path, fmt):

    try:
        df = (
            spark.read
            .format(fmt)
            .option("header",True)
            .option("inferSchema",True)
            .option("mode","PERMISSIVE")
            .option("columnNameOfCorruptRecord","_corrupt_record")
            .load(path)
        )

        validate_dataframe(df)

        return df.filter("_corrupt_record IS NULL")

    except Exception as e:
        logger.error(f"Load failed {path} {e}")
        raise
