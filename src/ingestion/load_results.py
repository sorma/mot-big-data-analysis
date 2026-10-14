from pyspark.sql import DataFrame, SparkSession

from ingestion.schemas import TEST_RESULT_SCHEMA


def load_test_results(
    spark: SparkSession,
    path: str,
    delimiter: str = ",",
) -> DataFrame:
    """
    Load MOT test result data into a Spark DataFrame.

    Parameters
    ----------
    spark:
        Active SparkSession.
    path:
        Path to the input MOT test result file or directory.
    delimiter:
        Field delimiter used by the input file.

    Returns
    -------
    DataFrame
        Spark DataFrame containing MOT test results.
    """

    return (
        spark.read
        .option("header", True)
        .option("delimiter", delimiter)
        .schema(TEST_RESULT_SCHEMA)
        .csv(path)
    )