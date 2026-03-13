
from pyspark.sql.functions import broadcast,round,col

def build_enriched_orders(orders,customers,products):

    orders = orders.repartition("customer_id")

    return (
        orders
        .join(broadcast(customers),"customer_id","left")
        .join(broadcast(products),"product_id","left")
        .withColumn("profit",round(col("profit"),2))
    )
