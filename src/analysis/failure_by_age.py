from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results
from preprocessing.clean_results import clean_test_results
from preprocessing.analytic_dataset import split_ambiguous_tests
from preprocessing.features import add_vehicle_age, add_vehicle_age_band


spark = (
    SparkSession.builder
    .appName("MOT Failure Rate by Vehicle Age")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# 1. Ingestion
raw_df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_202401.csv"
)


# 2. Cleaning
clean_df = clean_test_results(raw_df)


# 3. Remove ambiguous test IDs
analytic_df, ambiguous_df = split_ambiguous_tests(clean_df)


# 4. Feature engineering
df = add_vehicle_age(analytic_df)
df = add_vehicle_age_band(df)


# 5. Select the population for the analysis
analysis_df = (
    df
    .filter(F.col("test_class_id") == "4")
    .filter(F.col("test_type") == "NT")
    .filter(F.col("test_result").isin("P", "F", "PRS"))
)


# 6. Aggregate by age band
result_df = (
    analysis_df
    .groupBy("vehicle_age_band")
    .agg(
        F.count("*").alias("total_tests"),

        F.sum(
            F.when(F.col("test_result") == "P", 1).otherwise(0)
        ).alias("passed"),

        F.sum(
            F.when(F.col("test_result") == "F", 1).otherwise(0)
        ).alias("failed"),

        F.sum(
            F.when(F.col("test_result") == "PRS", 1).otherwise(0)
        ).alias("prs")
    )
    .withColumn(
        "final_failure_rate",
        F.round(
            F.col("failed") / F.col("total_tests") * 100,
            2
        )
    )
    .withColumn(
        "initial_failure_rate",
        F.round(
            (F.col("failed") + F.col("prs"))
            / F.col("total_tests") * 100,
            2
        )
    )
)


# 7. Order age bands
result_df = result_df.orderBy(
    F.when(F.col("vehicle_age_band") == "<3", 1)
    .when(F.col("vehicle_age_band") == "3-5", 2)
    .when(F.col("vehicle_age_band") == "6-10", 3)
    .when(F.col("vehicle_age_band") == "11-15", 4)
    .when(F.col("vehicle_age_band") == "16-20", 5)
    .when(F.col("vehicle_age_band") == "21-30", 6)
    .otherwise(7)
)


print("\n=== FAILURE RATE PER FASCIA DI ETÀ ===")

result_df.show(truncate=False)


spark.stop()