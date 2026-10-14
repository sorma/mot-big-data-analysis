from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results


spark = (
    SparkSession.builder
    .appName("MOT Data Quality Check")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_202401.csv"
)


print("\n=== DATASET ===")

total_rows = df.count()
distinct_test_ids = df.select("test_id").distinct().count()

print(f"Numero righe: {total_rows}")
print(f"Test ID distinti: {distinct_test_ids}")


print("\n=== TEST ID DUPLICATI ===")

duplicate_test_ids = (
    df
    .groupBy("test_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)

duplicate_test_ids.show(20, truncate=False)

print("\n=== DUPLICATI ESATTI ===")

distinct_rows = df.distinct().count()
exact_duplicate_rows = total_rows - distinct_rows

print(f"Righe totali: {total_rows}")
print(f"Righe completamente distinte: {distinct_rows}")
print(f"Righe duplicate esatte: {exact_duplicate_rows}")

distinct_df = df.distinct()
conflicting_test_ids = (
    distinct_df
    .groupBy("test_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)

print("\n=== TEST ID CON VALORI DIFFERENTI ===")

conflicting_test_ids.show(20, truncate=False)

conflicting_count = conflicting_test_ids.count()

print(f"Numero di test_id conflittuali: {conflicting_count}")

print("\n=== DETTAGLIO TEST ID 1586552547 ===")

(
    df
    .filter(F.col("test_id") == 1586552547)
    .distinct()
    .show(50, truncate=False)
)


spark.stop()