
from pyspark.sql.functions import sum,year

def build_profit_aggregation(df):

    df = df.withColumn("year",year("order_date"))

    return (
        df.groupBy(
            "year",
            "category",
            "sub_category",
            "customer_name"
        )
        .agg(sum("profit").alias("total_profit"))
    )
