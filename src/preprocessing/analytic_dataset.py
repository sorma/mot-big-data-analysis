from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def split_ambiguous_tests(df: DataFrame):
    """
    Split cleaned MOT test results into:
    - analytic dataset: test_ids associated with exactly one row
    - ambiguous dataset: test_ids associated with multiple distinct rows
    """

    test_id_counts = (
        df
        .groupBy("test_id")
        .count()
    )

    unique_test_ids = (
        test_id_counts
        .filter(F.col("count") == 1)
        .select("test_id")
    )

    ambiguous_test_ids = (
        test_id_counts
        .filter(F.col("count") > 1)
        .select("test_id")
    )

    analytic_df = df.join(
        unique_test_ids,
        on="test_id",
        how="inner"
    )

    ambiguous_df = df.join(
        ambiguous_test_ids,
        on="test_id",
        how="inner"
    )

    return analytic_df, ambiguous_df