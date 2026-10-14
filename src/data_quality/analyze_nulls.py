from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results


spark = (
    SparkSession.builder
    .appName("MOT Null Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_202401.csv"
)


null_counts = df.agg(
    *[
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in df.columns
    ]
)


print("\n=== VALORI NULL ===")

null_counts.show(truncate=False)
invalid_ids = df.filter(
    F.col("test_id").isNull()
    | F.col("vehicle_id").isNull()
)

print("\n=== RIGHE CON ID MANCANTI ===")
invalid_ids.show(30, truncate=False)

print(
    "Numero righe con almeno un ID mancante:",
    invalid_ids.count()
)

spark.stop()