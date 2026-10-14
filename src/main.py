from pyspark.sql import SparkSession

from analysis.age_distribution import analyze_age_distribution
from analysis.failure_by_age import analyze_failure_by_age
from analysis.failure_by_mileage import analyze_failure_by_mileage

PARQUET_PATH = (
    "/app/data/processed/parquet/test_results_2024"
)

RESULTS_PATH = (
    "/app/results/automotive"
)


def main():

    spark = (
        SparkSession.builder
        .appName("MOT 2024 Analysis")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")


    print("\n=== MOT 2024 ANALYSIS ===")


    # --------------------------------------------------
    # 1. Load processed dataset
    # --------------------------------------------------

    print("\n[1/4] Loading processed Parquet dataset...")

    df = spark.read.parquet(PARQUET_PATH)


    # --------------------------------------------------
    # 2. Vehicle age distribution
    # --------------------------------------------------

    print("\n[2/4] Vehicle age distribution...")

    age_distribution_df = analyze_age_distribution(df)

    age_distribution_df.show(
        truncate=False
    )

    # Save result as CSV
    (
        age_distribution_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{RESULTS_PATH}/age_distribution")
    )


    # --------------------------------------------------
    # 3. Failure rate by vehicle age
    # --------------------------------------------------

    print("\n[3/4] Failure rate by vehicle age...")

    failure_by_age_df = analyze_failure_by_age(df)

    failure_by_age_df.show(
        truncate=False
    )

    # Save result as CSV
    (
        failure_by_age_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{RESULTS_PATH}/failure_by_age")
    )


    # --------------------------------------------------
    # 4. Failure rate by mileage
    # --------------------------------------------------

    print("\n[4/4] Failure rate by mileage...")

    failure_by_mileage_df = analyze_failure_by_mileage(df)

    failure_by_mileage_df.show(
        truncate=False
    )

    # Save result as CSV
    (
        failure_by_mileage_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{RESULTS_PATH}/failure_by_mileage")
    )


    print("\n=== RESULTS SAVED ===")
    print(f"Results directory: {RESULTS_PATH}")


    spark.stop()


if __name__ == "__main__":
    main()