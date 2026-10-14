from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_vehicle_age(df: DataFrame) -> DataFrame:
    """
    Add vehicle age in completed years at the time of the MOT test.
    """
    return df.withColumn(
        "vehicle_age",
        F.floor(
            F.months_between(
                F.col("test_date"),
                F.col("first_use_date")
            ) / 12
        )
    )


def add_vehicle_age_band(df: DataFrame) -> DataFrame:
    """
    Group vehicle age into analytical age bands.

    Invalid or missing ages are kept separate instead of being
    silently assigned to a valid age band.
    """
    return df.withColumn(
        "vehicle_age_band",
        F.when(
            F.col("vehicle_age").isNull(),
            "unknown"
        )
        .when(
            F.col("vehicle_age") < 0,
            "invalid"
        )
        .when(F.col("vehicle_age") < 3, "<3")
        .when(F.col("vehicle_age") <= 5, "3-5")
        .when(F.col("vehicle_age") <= 10, "6-10")
        .when(F.col("vehicle_age") <= 15, "11-15")
        .when(F.col("vehicle_age") <= 20, "16-20")
        .when(F.col("vehicle_age") <= 30, "21-30")
        .otherwise(">30")
    )

def add_mileage_band(df: DataFrame) -> DataFrame:
    """
    Group MOT mileage into analytical mileage bands.

    Missing or invalid mileage values are kept separate.
    """
    return df.withColumn(
        "mileage_band",
        F.when(
            F.col("test_mileage").isNull(),
            "unknown"
        )
        .when(
            F.col("test_mileage") <= 0,
            "invalid"
        )
        .when(F.col("test_mileage") <= 25000, "0-25k")
        .when(F.col("test_mileage") <= 50000, "25-50k")
        .when(F.col("test_mileage") <= 75000, "50-75k")
        .when(F.col("test_mileage") <= 100000, "75-100k")
        .when(F.col("test_mileage") <= 150000, "100-150k")
        .when(F.col("test_mileage") <= 200000, "150-200k")
        .when(F.col("test_mileage") <= 300000, "200-300k")
        .otherwise(">300k")
    )