from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition
from shared.partitions import partition_daily


partition_so = StaticPartitionsDefinition(["TSO", "DSO", "ISO", "BSO", "NSO", "PSO"])
partition_5so = StaticPartitionsDefinition(["DSO", "ISO", "BSO", "NSO", "PSO"])
partition_2so = StaticPartitionsDefinition(["TSO", "DSO"])

partition_so_daily = MultiPartitionsDefinition(
    {
        "date": partition_daily,
        "so": partition_so,
    }
)
partition_5so_daily = MultiPartitionsDefinition(
    {
        "date": partition_daily,
        "so": partition_5so,
    }
)
partition_2so_daily = MultiPartitionsDefinition(
    {
        "date": partition_daily,
        "so": partition_2so,
    }
)