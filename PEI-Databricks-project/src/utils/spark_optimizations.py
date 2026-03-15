

def get_optimal_shuffle_partitions(data_size_mb, core_count, target_partition_mb=128, min_partitions=4):
    size_based = max(min_partitions, int(data_size_mb / target_partition_mb))
    core_based = max(min_partitions, core_count * 2)
    return max(size_based, core_based)


def apply_shuffle_partitions(spark, data_size_mb, core_count, target_partition_mb=128):
    partitions = get_optimal_shuffle_partitions(
        data_size_mb=data_size_mb,
        core_count=core_count,
        target_partition_mb=target_partition_mb
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    return partitions