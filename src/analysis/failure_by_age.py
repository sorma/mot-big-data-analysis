from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def analyze_failure_by_age(df: DataFrame) -> DataFrame:
    """
    Calculate MOT failure rates by vehicle age band.

    Population:
    - Class 4 vehicles
    - Normal tests (NT)
    - Outcomes P, F and PRS

    Returns:
    - total tests
    - passed tests
    - failed tests
    - PRS tests
    - final failure rate
    - initial failure rate
    """

    analysis_df = (
        df
        .filter(F.col("test_class_id") == "4")
        .filter(F.col("test_type") == "NT")
        .filter(F.col("test_result").isin("P", "F", "PRS"))
        .filter(
            ~F.col("vehicle_age_band").isin(
                "invalid",
                "unknown"
            )
        )
    )

    result_df = (
        analysis_df
        .groupBy("vehicle_age_band")
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
        F.when(F.col("vehicle_age_band") == "<3", 1)
        .when(F.col("vehicle_age_band") == "3-5", 2)
        .when(F.col("vehicle_age_band") == "6-10", 3)
        .when(F.col("vehicle_age_band") == "11-15", 4)
        .when(F.col("vehicle_age_band") == "16-20", 5)
        .when(F.col("vehicle_age_band") == "21-30", 6)
        .when(F.col("vehicle_age_band") == ">30", 7)
        .otherwise(8)
    )

    return result_df