from pyspark.sql import SparkSession

from ingestion.load_results import load_test_results
from preprocessing.clean_results import clean_test_results
from preprocessing.analytic_dataset import build_analytic_dataset
from preprocessing.features import (
    add_vehicle_age,
    add_vehicle_age_band,
    add_mileage_band,
)


INPUT_PATH = "/app/data/raw/test_results/test_result_2024*.csv"

OUTPUT_PATH = (
    "/app/data/processed/parquet/test_results_2024"
)


spark = (
    SparkSession.builder
    .appName("Build MOT 2024 Analytic Dataset")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n=== BUILDING MOT 2024 ANALYTIC DATASET ===")


# 1. Load raw data
print("\n[1/5] Loading raw CSV files...")

raw_df = load_test_results(
    spark,
    INPUT_PATH
)


# 2. Clean data
print("\n[2/5] Cleaning data...")

clean_df = clean_test_results(raw_df)


# 3. Remove ambiguous test IDs
print("\n[3/5] Removing ambiguous test IDs...")

analytic_df = build_analytic_dataset(clean_df)


# 4. Feature engineering
print("\n[4/5] Adding analytical features...")

analytic_df = add_vehicle_age(analytic_df)
analytic_df = add_vehicle_age_band(analytic_df)
analytic_df = add_mileage_band(analytic_df)


# 5. Write Parquet
print("\n[5/5] Writing Parquet dataset...")

(
    analytic_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("\n=== DATASET SUCCESSFULLY WRITTEN ===")
print(f"Output: {OUTPUT_PATH}")


spark.stop()