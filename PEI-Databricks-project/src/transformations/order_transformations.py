
from pyspark.sql.functions import broadcast,round,col

def build_enriched_orders(orders, customers, products, repartition_col=None):
    if repartition_col:
        orders = orders.repartition(repartition_col)

    return (
        orders
        .join(broadcast(customers), "customer_id", "left")
        .join(broadcast(products), "product_id", "left")
        .withColumn("profit", round(col("profit"), 2))
    )