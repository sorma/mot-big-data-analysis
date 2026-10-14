from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results


spark = (
    SparkSession.builder
    .appName("MOT Conflict Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_202401.csv"
)

# Rimuoviamo prima le copie perfettamente identiche
distinct_df = df.distinct()


columns_to_check = [
    column
    for column in df.columns
    if column != "test_id"
]


print("\n=== ANALISI COLONNE CONFLITTUALI ===")

for column in columns_to_check:

    conflicting_ids = (
        distinct_df
        .groupBy("test_id")
        .agg(
            F.countDistinct(column).alias("distinct_values")
        )
        .filter(F.col("distinct_values") > 1)
        .count()
    )

    print(
        f"{column}: "
        f"{conflicting_ids} test_id con valori differenti"
    )


spark.stop()