from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def analyze_failure_by_mileage(df: DataFrame) -> DataFrame:
    """
    Calculate MOT failure rates by mileage band.

    Population:
    - Class 4 vehicles
    - Normal tests (NT)
    - Outcomes P, F and PRS
    - Valid mileage
    """

    analysis_df = (
        df
        .filter(F.col("test_class_id") == "4")
        .filter(F.col("test_type") == "NT")
        .filter(F.col("test_result").isin("P", "F", "PRS"))
        .filter(
            ~F.col("mileage_band").isin(
                "invalid",
                "unknown"
            )
        )
    )

    result_df = (
        analysis_df
        .groupBy("mileage_band")
        .agg(
            F.count("*").alias("total_tests"),

            F.sum(
                F.when(
                    F.col("test_result") == "P",
                    1
                ).otherwise(0)
            ).alias("passed"),

            F.sum(
                F.when(
                    F.col("test_result") == "F",
                    1
                ).otherwise(0)
            ).alias("failed"),

            F.sum(
                F.when(
                    F.col("test_result") == "PRS",
                    1
                ).otherwise(0)
            ).alias("prs")
        )
        .withColumn(
            "final_failure_rate",
            F.round(
                F.col("failed")
                / F.col("total_tests")
                * 100,
                2
            )
        )
        .withColumn(
            "initial_failure_rate",
            F.round(
                (
                    F.col("failed")
                    + F.col("prs")
                )
                / F.col("total_tests")
                * 100,
                2
            )
        )
    )

    result_df = result_df.orderBy(
        F.when(F.col("mileage_band") == "0-25k", 1)
        .when(F.col("mileage_band") == "25-50k", 2)
        .when(F.col("mileage_band") == "50-75k", 3)
        .when(F.col("mileage_band") == "75-100k", 4)
        .when(F.col("mileage_band") == "100-150k", 5)
        .when(F.col("mileage_band") == "150-200k", 6)
        .when(F.col("mileage_band") == "200-300k", 7)
        .when(F.col("mileage_band") == ">300k", 8)
        .otherwise(9)
    )

    return result_df