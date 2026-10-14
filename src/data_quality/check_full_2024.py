from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results
from preprocessing.clean_results import clean_test_results


spark = (
    SparkSession.builder
    .appName("MOT Full 2024 Check")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# 1. Load all 2024 monthly files
raw_df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_2024*.csv"
)


print("\n=== FULL 2024 - RAW ===")

raw_count = raw_df.count()

print(f"Righe raw: {raw_count}")


# 2. Clean
clean_df = clean_test_results(raw_df)


print("\n=== FULL 2024 - CLEAN ===")

clean_count = clean_df.count()

print(f"Righe clean: {clean_count}")


# 3. Count how many rows are associated with each test_id
test_id_counts = (
    clean_df
    .groupBy("test_id")
    .count()
)


# 4. Calculate summary directly from the grouped data
summary = (
    test_id_counts
    .agg(
        F.sum(
            F.when(
                F.col("count") == 1,
                1
            ).otherwise(0)
        ).alias("unique_test_ids"),

        F.sum(
            F.when(
                F.col("count") > 1,
                1
            ).otherwise(0)
        ).alias("ambiguous_test_ids"),

        F.sum(
            F.when(
                F.col("count") > 1,
                F.col("count")
            ).otherwise(0)
        ).alias("ambiguous_rows")
    )
)


print("\n=== FULL 2024 - TEST ID SUMMARY ===")

summary.show(truncate=False)


# 5. Final consistency check
summary_row = summary.first()

unique_test_ids = summary_row["unique_test_ids"]
ambiguous_test_ids = summary_row["ambiguous_test_ids"]
ambiguous_rows = summary_row["ambiguous_rows"]

analytic_rows = unique_test_ids


print("\n=== FULL 2024 - ANALYTIC ===")
print(f"Righe analytic: {analytic_rows}")


print("\n=== FULL 2024 - AMBIGUOUS ===")
print(f"Righe ambiguous: {ambiguous_rows}")
print(f"Test ID ambigui: {ambiguous_test_ids}")


print("\n=== CONSISTENCY CHECK ===")
print(f"Clean:                {clean_count}")
print(f"Analytic + ambiguous: {analytic_rows + ambiguous_rows}")


spark.stop()