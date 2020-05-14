# ===== import: python function
from uuid import uuid4
import pyspark.sql.functions as f

# ===== import: palantir functions

# ===== import: our functions
from python.util import bp_constants
from python.util.bp_dateutils import filter_latest_runs

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def add_hist_col(df):
    '''
    This function:
        inserts new "history" column which records down the current timestamp
    '''
    return df.withColumn(bp_constants.HIST_DATETIME_COL, f.current_timestamp())


def get_dates(df, group_cols):
    '''
    This function:
        gets the distinct values of the columns passed

    Args:
        df (DataFrame): input data
        group_cols (list of strings): identifies the columns over which to extract distinct values
    '''
    spark_cols = [f.col(c) for c in group_cols]
    return df.select(*spark_cols).distinct()


def restrict_to_top_dates(df, top_n, date_col):
    '''
    This function:
        returns a dataset restricted to rows where date_col is in the top_n
        values found in date_col across the whole dataset. Equivalent to
        SELECT * FROM df WHERE date_col in (
            SELECT TOP top_n DISTINCT date_col FROM df)
    '''
    safe_col_name = 'Distinct_Dates_' + str(uuid4())  # use a prefix not just a bare uuid to make query plans readable
    df_distinct_dates = get_dates(df, [date_col]).withColumnRenamed(date_col, safe_col_name)
    df_top_dates = df_distinct_dates.sort(df_distinct_dates[safe_col_name].desc()).limit(top_n)
    return df.join(df_top_dates, df.Valuation_Date == df_top_dates[safe_col_name]).drop(safe_col_name)


def filter_latest_data_by_source(
        transform_input,
        transform_output,
        valuation_date_col=bp_constants.VALUATION_DATE_COL,
        source_col=bp_constants.SOURCE_COL,
        last_update_col=bp_constants.HIST_DATETIME_COL
        ):
    '''Entry point for creating history from transformed for multi-source datasets

    See filter_latest_data()
    '''
    return filter_latest_data(transform_input, transform_output, valuation_date_col, source_col, last_update_col)


def add_missing_columns(new_df, prev_df):
    '''
    This function:
        Adds columns present in new_df but not in prev_df

        This is used to make the history transform work when the schema changes.

    Args:
        new_df (DataFrame): DataFrame with the desired schema
        prev_df (DataFrame): DataFrame to which columns should be added

    Returns:
        prev_df conformed to the schema of new_df
    '''
    for col in new_df.columns:
        if col not in prev_df.columns:
            prev_df = prev_df.withColumn(col, f.lit(None))
    return prev_df


def filter_latest_data(
        transform_input,
        transform_output,
        valuation_date_col=bp_constants.VALUATION_DATE_COL,
        source_col=None,
        last_update_col=bp_constants.HIST_DATETIME_COL
        ):
    '''Entry point for creating history from transformed.

    This function:

        Takes a single transaction's worth of data and appends or replaces it
        in the history dataset.

        Sets the mode of the transform output appropriately.

    Args:
        transform_input (TransformInput): points to the latest transaction(s) in the transformed stage
        transform_output (TransformOutput): points to the output transaction in the history stage
        valuation_date_col (String): name of the column in which the valuation date is stored in transformed
        source_col (String): name of the column in which the data source is stored in transformed
                             (for multi-source pipelines only; should be None for single-source)
        last_update_col  (String): name of the column where the 'last updated' timestamp is maintained in history

    Returns:
        DataFrame: the updated history data
    '''
    new_df = add_hist_col(transform_input.dataframe())
    try:
        prev_df = transform_output.dataframe('previous', schema=transform_output.dataframe().schema)
    except Exception:
        prev_df = transform_input.dataframe()

    new_df, mode = filter_and_replace_common_dates(new_df, prev_df, valuation_date_col, source_col, last_update_col)
    if mode:
        transform_output.set_mode('replace')

    return new_df


def filter_and_replace_common_dates(new_df, prev_df, valuation_date_col, source_col, last_update_col):
    '''
        This function:

            Replaces data from prev_df with data from new_df where the valuation_date and
            source (for multi-source pipelines only) match.abs

            Copies data from prev_df and new_df where there is no such match.

            Not intended for use outside filter_latest_data().

        Args:

            new_df (DataFrame): contains the new data to be added to history from transformed
            prev_df (DataFrame): contains the 'old' state of history
            valuation_date_col (String): name of the column in which the valuation date is stored in transformed
            source_col (String): name of the column in which the data source is stored in transformed
                                (for multi-source pipelines only)
            last_update_col  (String): name of the column where the 'last updated' timestamp is maintained in history

        Returns:
            DataFrame: the updated state of history
    '''
    # Take only the latest HIST_DATETIME_COL(Hist_Date) for each VALUATION_DATE_COL (Valuation_Date),
    # for each source of data
    partition_cols = [valuation_date_col, source_col] if source_col else [valuation_date_col]
    new_df = filter_latest_runs(new_df, partition_cols, last_update_col)

    # get list of valuation dates to check if they existed in the previous view
    common_dates_df = get_common_dates(new_df, prev_df, partition_cols)
    if common_dates_df.count() > 0:
        prev_df = add_missing_columns(new_df, prev_df)
        new_df = combine_and_replace_latest_data(common_dates_df, new_df, prev_df,
                                                 valuation_date_col, source_col, last_update_col)
        mode = True
    else:
        mode = False
    return (new_df, mode)


def get_common_dates(df1, df2, group_cols):
    '''
    This function:
        Gets the intersection of the group_cols between new_df and prev_df.

        This is used to determine the common dates for the history transform,
        hence its name, but it actually does not rely on any of the columns
        containing a date.

        It is commutative in its arguments.  The group_cols must exist in both.

    Args:
        df1 (DataFrame): one DataFrame to process.
        df2 (DataFrame): the other DataFrame to process.
        group_cols (list of strings): the column names on which to intersect.

    Returns:
        DataFrame: the intersection of the group_cols between df1 and df2.
    '''
    df1_dates_df = get_dates(df1, group_cols)
    df2_dates_df = get_dates(df2, group_cols)
    common_dates_df = df1_dates_df.join(df2_dates_df, on=group_cols, how='inner')
    return common_dates_df


def combine_and_replace_latest_data(common_dates_df, new_df, prev_df, valuation_date_col, source_col, last_update_col):
    '''
    This function:
        Updates the history dataset with new information.

    Args:
        common_dates_df (DataFrame): contains the common dates by source between new_df and prev_df
        new_df (DataFrame): contains the new information to be added to the history
        prev_df (DataFrame): contains the state of the history to which information is to be added
        valuation_date_col (string): identifies the column containing valuation dates in the DataFrame parameters;
            the column identified must be of type DateType() in their schemata
        source_col (string): identifies the column containing source information in the DataFrame parameters;
            the column identified must be of type StringType() in their schemata
        last_update_col (string): identifies the time where the history's last update timestamp is kept;
            must be present in prev_df and of type TimestampType() in its schema

    Returns:
        DataFrame: the new state of history.
    '''
    if source_col:
        # get list of sources
        source_list = common_dates_df.select(source_col).distinct().collect()
        drop_source_col = False
    else:
        # No source column supplied, so create an empty one.
        # This is fairly cheap and should mostly be optimized away.
        source_list = [('',)]
        source_col = 'Source_' + str(uuid4())
        common_dates_df = common_dates_df.withColumn(source_col, f.lit(''))
        new_df = new_df.withColumn(source_col, f.lit(''))
        prev_df = prev_df.withColumn(source_col, f.lit(''))
        drop_source_col = True

    # loop through sources
    for (source,) in source_list:
        # If input_df valuation dates already exist in the output_data
        common_dates = [
            row[valuation_date_col]
            for row in common_dates_df.where(common_dates_df[source_col] == source).collect()
        ]

        # drop these rows from the output_df
        prev_df_source = prev_df.where(prev_df[source_col] == source)
        prev_df_source = prev_df_source[~prev_df_source[valuation_date_col].isin(common_dates)]

        # append the new values
        new_df = prev_df_source.union(new_df)

    # loop through sources removing them from the input
    for (source,) in source_list:
        prev_df = prev_df.where(prev_df[source_col] != source)  # inverse of boolean expr above

    # union what's left with what we previously calculated
    new_df = prev_df.union(new_df)

    if drop_source_col:
        new_df = new_df.drop(source_col)

    return new_df
