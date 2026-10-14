from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def analyze_age_distribution(df: DataFrame) -> DataFrame:
    """
    Calculate the distribution of MOT tests by vehicle age band.
    """

    result_df = (
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
            .when(F.col("vehicle_age_band") == ">30", 7)
            .when(F.col("vehicle_age_band") == "invalid", 8)
            .otherwise(9)
        )
    )

    return result_df