from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from datetime import date, datetime
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_constants import VALUATION_DATE_COL, HIST_DATETIME_COL
from pandas.testing import assert_frame_equal
import pytest


@pytest.fixture
def filter_latest_runs_data():

    schema = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Hist_Datetime', TimestampType(), True),
        StructField('Source', StringType(), True)
    ])

    # ======= create dataframe
    # +------------+------------+---------+
    # | Val_Date   | Hist_Date  | Source  |
    # +------------+------------+---------+
    # | D1         | HD1        | Endur   |
    # | D1         | HD1        | Power   |
    # | D1         | HD1        | Gas     |
    # | D1         | HD1        | Titan   |
    # | D1         | HD2        | Endur   |
    # | D1         | HD2        | Power   |
    # | D1         | HD2        | Titan   |
    # +------------+------------+---------+
    input_data = [
        (date(2019, 10, 1), datetime(2019, 10, 1, 6, 0, 0), 'Endur'),
        (date(2019, 10, 1), datetime(2019, 10, 1, 6, 0, 0), 'Power'),
        (date(2019, 10, 1), datetime(2019, 10, 1, 6, 0, 0), 'Gas'),
        (date(2019, 10, 1), datetime(2019, 10, 1, 6, 0, 0), 'Titan'),
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0), 'Endur'),
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0), 'Power'),
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0), 'Titan'),
    ]

    # ========= create desired_output_data
    # +------------+------------+---------+
    # | Val_Date   | Hist_Date  | Source  |
    # +------------+------------+---------+
    # | D1         | HD2        | Endur   |
    # | D1         | HD2        | Power   |
    # | D1         | HD1        | Gas     |
    # | D1         | HD2        | Titan   |
    # +------------+------------+---------+
    desired_output_data = [
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0), 'Endur'),
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0), 'Power'),
        (date(2019, 10, 1), datetime(2019, 10, 1, 6, 0, 0), 'Gas'),
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0), 'Titan'),
    ]
    return(schema, input_data, desired_output_data, [VALUATION_DATE_COL, 'Source'])


@pytest.fixture
def filter_latest_runs_data__no_source_column():

    schema = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Hist_Datetime', TimestampType(), True),
    ])

    # ======= create input data
    # +------------+------------+
    # | Val_Date   | Hist_Date  |
    # +------------+------------+
    # | D1         | HD1        |
    # | D1         | HD2        |
    # +------------+------------+
    input_data = [
        (date(2019, 10, 1), datetime(2019, 10, 1, 6, 0, 0)),
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0)),
    ]

    # ========= create desired_output_data
    # +------------+------------+
    # | Val_Date   | Hist_Date  |
    # +------------+------------+
    # | D1         | HD2        |
    # +------------+------------+
    desired_output_data = [
        (date(2019, 10, 1), datetime(2019, 10, 2, 6, 0, 0)),
    ]
    return(schema, input_data, desired_output_data, VALUATION_DATE_COL)


def test_filter_latest_runs(spark_session, filter_latest_runs_data):
    # Tests the main functionality of filter_latest_runs()
    filter_latest_runs_test_body(spark_session, filter_latest_runs_data)


def test_filter_latest_runs__no_source_column(spark_session, filter_latest_runs_data__no_source_column):
    # Expresses that filter_latest_runs() still has to work if there is no Source column
    filter_latest_runs_test_body(spark_session, filter_latest_runs_data__no_source_column)


def filter_latest_runs_test_body(spark_session, test_data_spec):
    (schema, input_data, desired_output_data, partition_cols) = test_data_spec
    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    desired_output_unsorted_df = spark_session.createDataFrame(data=desired_output_data, schema=schema)

    actual_output_unsorted_df = filter_latest_runs(input_df, partition_cols, HIST_DATETIME_COL)

    # Sort the desired output and actual output because we don't care about the order of the rows
    desired_output_df = desired_output_unsorted_df.sort(desired_output_unsorted_df.columns)
    actual_output_df = actual_output_unsorted_df.sort(actual_output_unsorted_df.columns)

    print("===============desired:")
    print(desired_output_df.collect())
    print("===============output:")
    print(actual_output_df.collect())

    assert_frame_equal(desired_output_df.toPandas(), actual_output_df.toPandas())
