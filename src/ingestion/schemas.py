from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


TEST_RESULT_SCHEMA = StructType([
    StructField("test_id", LongType(), True),
    StructField("vehicle_id", LongType(), True),
    StructField("test_date", StringType(), True),
    StructField("test_class_id", StringType(), True),
    StructField("test_type", StringType(), True),
    StructField("test_result", StringType(), True),
    StructField("test_mileage", DoubleType(), True),
    StructField("postcode_area", StringType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("colour", StringType(), True),
    StructField("fuel_type", StringType(), True),
    StructField("cylinder_capacity", LongType(), True),
    StructField("first_use_date", StringType(), True),
    StructField("completed_date", StringType(), True),
])