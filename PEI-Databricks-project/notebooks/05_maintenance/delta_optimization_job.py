
import sys
import os


PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/Workspace/Users/s.karthikeyan1100@gmail.com/dataprojects/PEI-Databricks-project"
)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from src.utils.delta_optimization import run_delta_maintenance
from src.utils.config import (
    RAW_CUSTOMERS,
    RAW_PRODUCTS,
    RAW_ORDERS,
    ENRICHED_CUSTOMERS,
    ENRICHED_PRODUCTS,
    ENRICHED_ORDERS,
    AGG_PROFIT
)


table_configs = [
    {
        "table_name": RAW_CUSTOMERS,
        "zorder_cols": ["customer_id"],
        "retention_hours": 168
    },
    {
        "table_name": RAW_PRODUCTS,
        "zorder_cols": ["product_id", "category"],
        "retention_hours": 168
    },
    {
        "table_name": RAW_ORDERS,
        "zorder_cols": ["order_id", "customer_id", "product_id"],
        "retention_hours": 168
    },
    {
        "table_name": ENRICHED_CUSTOMERS,
        "zorder_cols": ["customer_id"],
        "retention_hours": 168
    },
    {
        "table_name": ENRICHED_PRODUCTS,
        "zorder_cols": ["product_id", "category"],
        "retention_hours": 168
    },
    {
        "table_name": ENRICHED_ORDERS,
        "zorder_cols": ["order_id", "customer_id", "product_id"],
        "retention_hours": 168
    },
    {
        "table_name": AGG_PROFIT,
        "zorder_cols": ["year", "category", "sub_category"],
        "retention_hours": 168
    }
]

run_delta_maintenance(spark, table_configs)

print("[SUCCESS] OPTIMIZE and VACUUM completed for all configured tables")