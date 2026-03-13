from pyspark.sql.functions import sum, year, to_date

def build_profit_aggregation(df):
    # Use to_date with correct format and tolerate malformed input
    df = df.withColumn("order_date_parsed", to_date("order_date", "d/M/yyyy"))
    df = df.withColumn("year", year("order_date_parsed"))

    return (
        df.groupBy(
            "year",
            "category",
            "sub_category",
            "customer_name"
        )
        .agg(sum("profit").alias("total_profit"))
    )
