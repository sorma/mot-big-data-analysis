from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.load_results import load_test_results
from preprocessing.clean_results import clean_test_results
from preprocessing.analytic_dataset import split_ambiguous_tests
from preprocessing.features import add_vehicle_age, add_vehicle_age_band


spark = (
    SparkSession.builder
    .appName("MOT Age Distribution")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_df = load_test_results(
    spark,
    "/app/data/raw/test_results/test_result_202401.csv"
)

clean_df = clean_test_results(raw_df)

analytic_df, ambiguous_df = split_ambiguous_tests(clean_df)

df = add_vehicle_age(analytic_df)
df = add_vehicle_age_band(df)


print("\n=== DISTRIBUZIONE PER FASCIA DI ETÀ ===")

(
    df
    .groupBy("vehicle_age_band")
    .count()
    .orderBy(
        F.when(F.col("vehicle_age_band") == "<3", 1)
        .when(F.col("vehicle_age_band") == "3-5", 2)
        .when(F.col("vehicle_age_band") == "6-10", 3)
        .when(F.col("vehicle_age_band") == "11-15", 4)
        .when(F.col("vehicle_age_band") == "16-20", 5)
        .when(F.col("vehicle_age_band") == "21-30", 6)
        .otherwise(7)
    )
    .show()
)


spark.stop()