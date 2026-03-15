from typing import List, Dict


def optimize_table(spark, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        print(f"[SKIP] Table does not exist: {table_name}")
        return

    try:
        sql_stmt = f"OPTIMIZE {table_name}"
        print(f"[INFO] Running: {sql_stmt}")
        spark.sql(sql_stmt)
        print(f"[SUCCESS] OPTIMIZE completed for {table_name}")
    except Exception as e:
        print(f"[ERROR] OPTIMIZE failed for {table_name}: {e}")
        raise


def vacuum_table(spark, table_name: str, retention_hours: int = 168) -> None:
    if not spark.catalog.tableExists(table_name):
        print(f"[SKIP] Table does not exist: {table_name}")
        return

    try:
        sql_stmt = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
        print(f"[INFO] Running: {sql_stmt}")
        spark.sql(sql_stmt)
        print(f"[SUCCESS] VACUUM completed for {table_name}")
    except Exception as e:
        print(f"[ERROR] VACUUM failed for {table_name}: {e}")
        raise


def run_delta_maintenance(spark, table_configs: List[Dict]) -> None:
    if not table_configs:
        print("[WARN] No table configs provided")
        return

    for cfg in table_configs:
        table_name = cfg["table_name"]
        retention_hours = cfg.get("retention_hours", 168)

        print("=" * 100)
        print(f"[START] Delta maintenance for table: {table_name}")

        optimize_table(spark, table_name)
        vacuum_table(spark, table_name, retention_hours=retention_hours)

        print(f"[END] Delta maintenance completed for table: {table_name}")
        print("=" * 100)