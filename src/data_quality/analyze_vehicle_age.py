from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results
from preprocessing.clean_results import clean_test_results
from preprocessing.features import add_vehicle_age


spark = (
    SparkSession.builder
    .appName("MOT Vehicle Age Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_202401.csv"
)

clean_df = clean_test_results(raw_df)

df = add_vehicle_age(clean_df)


print("\n=== VEICOLI CON ETÀ > 100 ANNI ===")

(
    df
    .filter(F.col("vehicle_age") > 100)
    .select(
        "test_id",
        "vehicle_id",
        "make",
        "model",
        "test_date",
        "first_use_date",
        "vehicle_age",
        "test_result"
    )
    .orderBy(F.desc("vehicle_age"))
    .show(100, truncate=False)
)


print("\n=== FIRST USE DATE PIÙ ANTICHE ===")

(
    df
    .groupBy("first_use_date")
    .count()
    .orderBy("first_use_date")
    .show(30, truncate=False)
)

print("\n=== DISTRIBUZIONE ETÀ ELEVATE ===")

df.agg(
    F.sum(F.when(F.col("vehicle_age") > 30, 1).otherwise(0)).alias("over_30"),
    F.sum(F.when(F.col("vehicle_age") > 40, 1).otherwise(0)).alias("over_40"),
    F.sum(F.when(F.col("vehicle_age") > 50, 1).otherwise(0)).alias("over_50"),
    F.sum(F.when(F.col("vehicle_age") > 60, 1).otherwise(0)).alias("over_60"),
    F.sum(F.when(F.col("vehicle_age") > 80, 1).otherwise(0)).alias("over_80"),
    F.sum(F.when(F.col("vehicle_age") > 100, 1).otherwise(0)).alias("over_100")
).show()


spark.stop()