from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PARQUET_PATH = (
    "/app/data/processed/parquet/test_results_2024"
)


spark = (
    SparkSession.builder
    .appName("Analyze MOT Mileage")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n=== LOADING ANALYTIC DATASET ===")

df = spark.read.parquet(PARQUET_PATH)


# --------------------------------------------------
# 1. Basic mileage quality
# --------------------------------------------------

print("\n=== MILEAGE QUALITY ===")

df.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("test_mileage").isNull(),
            1
        ).otherwise(0)
    ).alias("null_mileage"),

    F.sum(
        F.when(
            F.col("test_mileage") < 0,
            1
        ).otherwise(0)
    ).alias("negative_mileage"),

    F.sum(
        F.when(
            F.col("test_mileage") == 0,
            1
        ).otherwise(0)
    ).alias("zero_mileage")
).show(truncate=False)


# --------------------------------------------------
# 2. Mileage descriptive statistics
# --------------------------------------------------

print("\n=== MILEAGE STATISTICS ===")

df.select("test_mileage").summary(
    "count",
    "mean",
    "stddev",
    "min",
    "25%",
    "50%",
    "75%",
    "max"
).show(truncate=False)


# --------------------------------------------------
# 3. High-mileage values
# --------------------------------------------------

print("\n=== HIGH MILEAGE COUNTS ===")

df.select(
    F.sum(
        F.when(F.col("test_mileage") > 100000, 1)
        .otherwise(0)
    ).alias("over_100k"),

    F.sum(
        F.when(F.col("test_mileage") > 200000, 1)
        .otherwise(0)
    ).alias("over_200k"),

    F.sum(
        F.when(F.col("test_mileage") > 300000, 1)
        .otherwise(0)
    ).alias("over_300k"),

    F.sum(
        F.when(F.col("test_mileage") > 500000, 1)
        .otherwise(0)
    ).alias("over_500k"),

    F.sum(
        F.when(F.col("test_mileage") > 1000000, 1)
        .otherwise(0)
    ).alias("over_1m")
).show(truncate=False)


# --------------------------------------------------
# 4. Largest mileage values
# --------------------------------------------------

print("\n=== HIGHEST MILEAGE RECORDS ===")

(
    df
    .select(
        "test_id",
        "vehicle_id",
        "make",
        "model",
        "test_date",
        "first_use_date",
        "vehicle_age",
        "test_mileage",
        "test_result"
    )
    .orderBy(
        F.col("test_mileage").desc_nulls_last()
    )
    .show(20, truncate=False)
)

print("\n=== EXTREME MILEAGE FREQUENCIES ===")

(
    df
    .filter(F.col("test_mileage") >= 500000)
    .groupBy("test_mileage")
    .count()
    .orderBy(F.col("test_mileage").desc())
    .show(50, truncate=False)
)


print("\n=== EXACT 999999 COUNT ===")

df.select(
    F.sum(
        F.when(
            F.col("test_mileage") == 999999,
            1
        ).otherwise(0)
    ).alias("exact_999999")
).show()

spark.stop()