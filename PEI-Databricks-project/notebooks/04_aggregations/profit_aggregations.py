
from src.transformations.aggregation_transformations import build_profit_aggregation
from src.utils.config import *

orders = spark.table(ENRICHED_ORDERS)

agg = build_profit_aggregation(orders)

agg.write.format("delta").mode("overwrite").saveAsTable(AGG_PROFIT)
