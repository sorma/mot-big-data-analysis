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
    """

    return df.withColumn(
        "vehicle_age_band",
        F.when(F.col("vehicle_age") < 3, "<3")
        .when(F.col("vehicle_age") <= 5, "3-5")
        .when(F.col("vehicle_age") <= 10, "6-10")
        .when(F.col("vehicle_age") <= 15, "11-15")
        .when(F.col("vehicle_age") <= 20, "16-20")
        .when(F.col("vehicle_age") <= 30, "21-30")
        .otherwise(">30")
    )