from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_test_results(df: DataFrame) -> DataFrame:
    """
    Clean MOT test result data.

    Operations:
    - remove exact duplicate rows
    - remove records without test_id or vehicle_id
    - safely parse date and timestamp columns
    """

    cleaned_df = (
        df
        .dropDuplicates()
        .filter(
            F.col("test_id").isNotNull()
            & F.col("vehicle_id").isNotNull()
        )
        .withColumn(
            "test_date",
            F.try_to_timestamp(
                F.col("test_date"),
                F.lit("yyyy-MM-dd")
            ).cast("date")
        )
        .withColumn(
            "first_use_date",
            F.try_to_timestamp(
                F.col("first_use_date"),
                F.lit("yyyy-MM-dd")
            ).cast("date")
        )
        .withColumn(
            "completed_date",
            F.try_to_timestamp(
                F.col("completed_date"),
                F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX")
            )
        )
    )

    return cleaned_df