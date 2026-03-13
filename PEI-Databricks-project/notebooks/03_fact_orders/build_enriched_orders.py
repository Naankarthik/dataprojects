
from src.transformations.order_transformations import build_enriched_orders
from src.utils.config import *

orders = spark.table(RAW_ORDERS)
customers = spark.table(ENRICHED_CUSTOMERS)
products = spark.table(ENRICHED_PRODUCTS)

enriched = build_enriched_orders(orders,customers,products)

enriched.write.format("delta").mode("overwrite").saveAsTable(ENRICHED_ORDERS)
