from src.utils.logger import logger

def validate_dataframe(df):

    try:

        # Check if dataset empty
        if df.limit(1).count() == 0:
            raise ValueError("Dataset is empty")

    except Exception as e:

        logger.error(f"Data validation failed {e}")
        raise