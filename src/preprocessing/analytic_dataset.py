from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def get_ambiguous_test_ids(df: DataFrame) -> DataFrame:
    """
    Return test_ids associated with more than one distinct cleaned row.
    """
    return (
        df
        .groupBy("test_id")
        .count()
        .filter(F.col("count") > 1)
        .select("test_id")
    )


def build_analytic_dataset(df: DataFrame) -> DataFrame:
    """
    Build the analytic MOT dataset by excluding ambiguous test_ids.

    A test_id is considered ambiguous when it is associated with
    more than one distinct cleaned row.
    """
    ambiguous_test_ids = get_ambiguous_test_ids(df)

    return (
        df
        .join(
            ambiguous_test_ids,
            on="test_id",
            how="left_anti"
        )
    )