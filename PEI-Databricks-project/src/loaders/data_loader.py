from src.utils.logger import logger
from src.utils.error_handler import validate_dataframe
from src.utils.column_utils import clean_column_names
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def load_data(path, fmt):
    try:
        if fmt == "excel":
            import pandas as pd
            pdf = pd.read_excel(path)
            if "phone" in pdf.columns:
                pdf["phone"] = pdf["phone"].astype(str)
            df = spark.createDataFrame(pdf)
        elif fmt == "json":
            df = (
                spark.read
                .format("json")
                .option("multiline", True)
                .load(path)
            )
        elif fmt == "csv":
            df = (
                spark.read
                .format("csv")
                .option("header", True)
                .option("inferSchema", True)
                .load(path)
            )
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        df = clean_column_names(df)
        validate_dataframe(df)
        return df

    except Exception as e:
        logger.error(f"Load failed with error: {e}")