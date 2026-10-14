from pyspark.sql import SparkSession


PARQUET_PATH = "/app/data/processed/parquet/test_results_2024"


spark = (
    SparkSession.builder
    .appName("Check MOT 2024 Parquet")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n=== LOADING PARQUET ===")

df = spark.read.parquet(PARQUET_PATH)


print("\n=== ROW COUNT ===")

row_count = df.count()

print(f"Righe Parquet: {row_count}")


print("\n=== SCHEMA ===")

df.printSchema()


print("\n=== SAMPLE ===")

df.show(5, truncate=False)


print("\n=== AGE QUALITY ===")

df.groupBy("vehicle_age_band").count().show(truncate=False)


spark.stop()