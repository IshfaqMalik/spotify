import pyspark.sql.functions as F


def to_datestamp(df, input_col, output_col):
    """
    Convert a timestamp column to a datestamp column.

    Parameters:
    df (DataFrame): The input DataFrame.
    input_col (str): The name of the input timestamp column.
    output_col (str): The name of the output datestamp column.

    Returns:
    DataFrame: The DataFrame with the new datestamp column.
    """
    return df.withColumn(output_col, F.to_timestamp(F.col(input_col)))


def to_date(df, input_col, output_col):
    """
    Convert a timestamp column to a date column.

    Parameters:
    df (DataFrame): The input DataFrame.
    input_col (str): The name of the input timestamp column.
    output_col (str): The name of the output date column.

    Returns:
    DataFrame: The DataFrame with the new date column.
    """
    return df.withColumn(output_col, F.to_date(F.col(input_col)))
