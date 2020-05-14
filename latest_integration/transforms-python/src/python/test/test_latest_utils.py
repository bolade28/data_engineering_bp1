# ===== import: python modules
from datetime import date, datetime
from pandas.testing import assert_frame_equal
import pytest
from unittest.mock import Mock

# ===== import: pyspark modules
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DoubleType
import pyspark.sql.functions as f

# ===== import: palantir modules

# ===== import: our modules
from python.util.bp_historical_file_utils import restrict_to_top_dates, \
    combine_and_replace_latest_data, get_common_dates, filter_and_replace_common_dates, \
    filter_latest_data, filter_latest_data_by_source, add_hist_col
from python.util.bp_constants import VALUATION_DATE_COL, SOURCE_COL, HIST_DATETIME_COL


@pytest.fixture
def uneven_dates_test_data():
    df_schema = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Hist_Datetime', TimestampType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True)
    ])

    # ... construct new_df
    #     +------+----------+--------+-------+
    #     | date | hist_date| source | value |
    #     +------+----------+--------+-------+
    #     | D1   | D1       |power   | 1     |
    #     | D1   | D1       | gas    | 2     |
    #     | D2   | D1       |power   | 3     |
    #     +------+----------+--------+-------+
    new_df_data = [(date(2019, 10, 1), datetime(2019, 10, 1, 1, 0, 0), 'power', 1),
                   (date(2019, 10, 1), datetime(2019, 10, 1, 1, 0, 0), 'gas', 2),
                   (date(2019, 10, 2), datetime(2019, 10, 1, 1, 0, 0), 'power', 3)]

    # ... construct prev_df
    #     +------+----------+--------+-------+
    #     | date | hist_date| source | value |
    #     +------+----------+--------+-------+
    #     | D2   | D1       |power   | 5     |
    #     | D2   | D1       | gas    | 4     |
    #     +------+----------+--------+-------+
    prev_df_data = [(date(2019, 10, 2), datetime(2019, 10, 1, 1, 0, 0), 'power', 5),
                    (date(2019, 10, 2), datetime(2019, 10, 1, 1, 0, 0), 'gas', 4)]

    # Construct desired_output_df
    #     +------+----------+--------+-------+
    #     | date | hist_date| source | value |
    #     +------+----------+--------+-------+
    #     | D1   | D1       | power  | 1     |
    #     | D1   | D1       | gas    | 2     |
    #     | D2   | D1       | power  | 3     |
    #     | D2   | D1       | gas    | 4     |
    #     +------+----------+--------+-------+

    desired_output_df_data = [(date(2019, 10, 1),  datetime(2019, 10, 1, 1, 0, 0), 'power', 1),
                              (date(2019, 10, 1), datetime(2019, 10, 1, 1, 0, 0), 'gas', 2),
                              (date(2019, 10, 2),  datetime(2019, 10, 1, 1, 0, 0), 'power', 3),
                              (date(2019, 10, 2),  datetime(2019, 10, 1, 1, 0, 0), 'gas', 4)]

    return (df_schema, new_df_data, prev_df_data, desired_output_df_data)


def test_filter_and_replace_common_dates__missing_data_uneven_val_dates(spark_session, uneven_dates_test_data):
    (df_schema, new_df_data, prev_df_data, desired_output_df_data) = uneven_dates_test_data

    new_df = spark_session.createDataFrame(new_df_data, df_schema)
    prev_df = spark_session.createDataFrame(prev_df_data, df_schema)
    desired_output_df_unsorted = spark_session.createDataFrame(desired_output_df_data, df_schema)
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    (output_df_unsorted, mode) = filter_and_replace_common_dates(
        new_df, prev_df, VALUATION_DATE_COL, SOURCE_COL, HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)
    assert(desired_output_df.schema == output_df.schema)

    assert(mode)

    assert(desired_output_df.count() == output_df.count())

    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())


def test_filter_and_replace_common_dates__not_by_source(spark_session, uneven_dates_test_data):
    (df_schema, new_df_data, prev_df_data, _) = uneven_dates_test_data

    new_df = spark_session.createDataFrame(new_df_data, df_schema)
    prev_df = spark_session.createDataFrame(prev_df_data, df_schema)
    desired_output_df_unsorted = spark_session.createDataFrame(new_df_data, df_schema)  # correct when not by source
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    output_df_unsorted, mode = filter_and_replace_common_dates(new_df,
                                                               prev_df,
                                                               VALUATION_DATE_COL,
                                                               None,
                                                               HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)
    assert(desired_output_df.schema == output_df.schema)

    assert(mode)

    assert(desired_output_df.count() == output_df.count())

    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())


def test_combine_and_replace_latest_data__missing_data_uneven_val_dates(spark_session, uneven_dates_test_data):
    df_schema, new_df_data, prev_df_data, desired_output_df_data = uneven_dates_test_data

    new_df = spark_session.createDataFrame(new_df_data, df_schema)
    prev_df = spark_session.createDataFrame(prev_df_data, df_schema)
    desired_output_df_unsorted = spark_session.createDataFrame(desired_output_df_data, df_schema)
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    common_dates_df = get_common_dates(new_df, prev_df, [VALUATION_DATE_COL, SOURCE_COL])

    output_df_unsorted = combine_and_replace_latest_data(
        common_dates_df, new_df, prev_df, VALUATION_DATE_COL, SOURCE_COL, HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)
    assert(desired_output_df.schema == output_df.schema)
    assert(desired_output_df.count() == output_df.count())

    # assert that output_df == desired_output_df
    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())


def test_combine_and_replace_latest_data__missing_data_uneven_val_dates_not_by_source(
        spark_session, uneven_dates_test_data):

    schema, new_data, prev_data, desired_output_df_data = uneven_dates_test_data

    new_df = spark_session.createDataFrame(new_data, schema)
    prev_df = spark_session.createDataFrame(prev_data, schema)
    desired_output_df_unsorted = spark_session.createDataFrame(new_data, schema)  # correct when not by source
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    common_dates_df = get_common_dates(new_df, prev_df, [VALUATION_DATE_COL])

    output_df_unsorted = combine_and_replace_latest_data(
        common_dates_df, new_df, prev_df, VALUATION_DATE_COL, None, HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)
    assert(desired_output_df.schema == output_df.schema)
    assert(desired_output_df.count() == output_df.count())

    # assert that output_df == desired_output_df
    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())

# =================================================================================================================
@pytest.fixture
def common_dates__basic_input_data():
    schema = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True)
    ])

    # Construct input data
    # ... construct new_df
    #     +------+--------+-------+
    #     | date | source | value |
    #     +------+--------+-------+
    #     | D1   | power  | 1     |
    #     | D1   | gas    | 2     |
    #     | D2   | power  | 3     |
    #     +------+--------+-------+
    new_df_data = [(date(2019, 10, 11), 'power', 1),
                   (date(2019, 10, 10), 'gas', 2),
                   (date(2019, 10, 10), 'power', 3)]

    # ... construct prev_df
    #     +------+--------+-------+
    #     | date | source | value |
    #     +------+--------+-------+
    #     | D2   | power  | 5     |
    #     | D2   | gas    | 4     |
    #     +------+--------+-------+
    prev_df_data = [(date(2019, 10, 10), 'power', 5),
                    (date(2019, 10, 10), 'gas', 4)]

    return (schema, new_df_data, prev_df_data)


@pytest.fixture
def common_dates__basic_output_data():
    desired_output_df_schema = StructType([
        StructField('Source', StringType(), True),
        StructField('Valuation_Date', DateType(), True)
    ])

    # Construct desired_output_df
    # ... construct desired_output_df
    #     +--------+------+
    #     | source | date |
    #     +--------+------+
    #     | power  | D2   |
    #     | gas    | D2   |
    #     +--------+------+
    desired_output_df_data = [('gas', date(2019, 10, 10)),
                              ('power', date(2019, 10, 10))
                              ]

    return(desired_output_df_schema, desired_output_df_data)


@pytest.fixture
def common_dates__by_source_input_data():
    df_schema = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True)
    ])

    # Construct input data
    # ... construct new_df
    #     +------+--------+-------+
    #     | date | source | value |
    #     +------+--------+-------+
    #     | D1   | power  | 1     |
    #     | D1   | gas    | 2     |
    #     | D2   | power  | 3     |
    #     +------+--------+-------+
    new_df_data = [(date(2019, 10, 11), 'power', 1),
                   (date(2019, 10, 10), 'gas', 2),
                   (date(2019, 10, 10), 'power', 3)]

    # ... construct prev_df
    #     +------+--------+-------+
    #     | date | source | value |
    #     +------+--------+-------+
    #     | D2   | power  | 5     |
    #     +------+--------+-------+
    prev_df_data = [(date(2019, 10, 10), 'power', 5)]
    return(df_schema, new_df_data, prev_df_data)


@pytest.fixture
def common_dates__by_source_output_data():
    desired_output_df_schema = StructType([
        StructField('Source', StringType(), True),
        StructField('Valuation_Date', DateType(), True)
    ])

    # Construct desired_output_df
    # ... construct desired_output_df
    #     +--------+------+
    #     | source | date |
    #     +--------+------+
    #     | power  | D2   |
    #     +--------+------+
    desired_output_df_data = [('power', date(2019, 10, 10))]
    return(desired_output_df_schema, desired_output_df_data)


def common_dates_test_body(spark_session, common_dates_input_data, common_dates_output_data, use_source):
    (df_schema, new_df_data, prev_df_data) = common_dates_input_data
    (desired_output_df_schema, desired_output_df_data) = common_dates_output_data

    new_df = spark_session.createDataFrame(new_df_data, df_schema)
    prev_df = spark_session.createDataFrame(prev_df_data, df_schema)
    desired_output_df = spark_session.createDataFrame(data=desired_output_df_data, schema=desired_output_df_schema)

    cols = [VALUATION_DATE_COL, SOURCE_COL] if use_source else [VALUATION_DATE_COL]

    if not use_source:
        desired_output_df = desired_output_df.drop(SOURCE_COL).dropDuplicates()

    output_df = get_common_dates(new_df, prev_df, cols).select(*cols)

    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas(), check_like=True)


def test_common_dates__basic(spark_session, common_dates__basic_input_data, common_dates__basic_output_data):
    common_dates_test_body(
        spark_session, common_dates__basic_input_data, common_dates__basic_output_data, use_source=True)


def test_common_dates__by_source(
        spark_session, common_dates__by_source_input_data, common_dates__by_source_output_data):
    common_dates_test_body(
        spark_session, common_dates__by_source_input_data, common_dates__by_source_output_data, use_source=True)


def test_common_dates__basic__ignore_source(
        spark_session, common_dates__basic_input_data, common_dates__basic_output_data):
    common_dates_test_body(
        spark_session, common_dates__basic_input_data, common_dates__basic_output_data, use_source=False)


def test_common_dates__by_source__ignore_source(
        spark_session, common_dates__by_source_input_data, common_dates__by_source_output_data):
    common_dates_test_body(
        spark_session, common_dates__by_source_input_data, common_dates__by_source_output_data, use_source=False)


@pytest.fixture
def restrict_top_input_data():
    return get_restrict_top_input_data()


def get_restrict_top_input_data():
    schema = StructType([
        StructField(VALUATION_DATE_COL, DateType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True)
    ])

    data = [
        (date(2019, 10, 11), 'power', 1),
        (date(2019, 10, 10), 'power', 2),
        (date(2019, 10, 10), 'power', 3),
        (date(2019, 10,  9), 'power', 4),
    ]

    return(schema, data)


@pytest.fixture
def restrict_top_output_data_1():
    (schema, data) = get_restrict_top_input_data()
    return(schema, data[0:1])


@pytest.fixture
def restrict_top_output_data_2():
    (schema, data) = get_restrict_top_input_data()
    return(schema, data[0:3])


def test_restrict_to_top_dates_1(
        spark_session,
        restrict_top_input_data,
        restrict_top_output_data_1,
        ):
    (input_schema, input_data) = restrict_top_input_data
    (desired_output_schema, desired_output_data) = restrict_top_output_data_1

    input_df = spark_session.createDataFrame(input_data, input_schema)

    desired_output_df = spark_session.createDataFrame(data=desired_output_data, schema=desired_output_schema)

    output_df = restrict_to_top_dates(input_df, 1, VALUATION_DATE_COL)

    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())


def test_restrict_to_top_dates_2(
        spark_session,
        restrict_top_input_data,
        restrict_top_output_data_2,
        ):
    (input_schema, input_data) = restrict_top_input_data
    (desired_output_schema, desired_output_data) = restrict_top_output_data_2

    input_df = spark_session.createDataFrame(input_data, input_schema)

    desired_output_df = spark_session.createDataFrame(data=desired_output_data, schema=desired_output_schema)

    output_df = restrict_to_top_dates(input_df, 2, VALUATION_DATE_COL)

    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())


def test_filter_latest_data_and_write_back(spark_session, uneven_dates_test_data):
    schema, new_data, prev_data, desired_output_data = uneven_dates_test_data
    input_df = spark_session.createDataFrame(new_data, schema)
    prev_df = spark_session.createDataFrame(prev_data, schema)
    desired_output_df = spark_session.createDataFrame(desired_output_data, schema).drop(HIST_DATETIME_COL)

    transform_input = Mock()
    transform_output = Mock()

    transform_input.dataframe.return_value = input_df
    transform_output.dataframe.return_value = prev_df

    output_df = filter_latest_data_by_source(transform_input, transform_output)

    output_df = output_df.drop(HIST_DATETIME_COL)
    assert_frame_equal(
        desired_output_df.sort(desired_output_df.columns).toPandas(),
        output_df.sort(output_df.columns).toPandas(),
        check_like=True)
    transform_output.set_mode.assert_called_with('replace')


def test_filter_latest_data_non_exposure(spark_session, uneven_dates_test_data):
    schema, new_data, prev_data, _ = uneven_dates_test_data
    input_df = spark_session.createDataFrame(new_data, schema)
    prev_df = spark_session.createDataFrame(prev_data, schema)
    desired_output_df = input_df.drop(HIST_DATETIME_COL)  # Note this is the expected output when ignoring source column
    input_df.withColumn(HIST_DATETIME_COL, f.lit(datetime(2019, 11, 1, 0, 0, 0)))

    transform_input = Mock()
    transform_output = Mock()

    transform_input.dataframe.return_value = input_df
    transform_output.dataframe.return_value = prev_df

    output_df = filter_latest_data(transform_input, transform_output)

    output_df = output_df.drop(HIST_DATETIME_COL)
    assert_frame_equal(
        desired_output_df.sort(desired_output_df.columns).toPandas(),
        output_df.sort(output_df.columns).toPandas(),
        check_like=True)
    transform_output.set_mode.assert_called_with('replace')


def test_add_hist_col(spark_session, common_dates__basic_input_data):
    schema, new_data, _ = common_dates__basic_input_data
    input_df = spark_session.createDataFrame(new_data, schema)

    assert(HIST_DATETIME_COL not in input_df.columns)
    input_df = add_hist_col(input_df)
    assert(HIST_DATETIME_COL in input_df.columns)
    assert(input_df.schema[HIST_DATETIME_COL].dataType == TimestampType())

# Test data to simulate the "edge case" observed during Dec 2019
# Nov 27 = D1
# Nov 28 = D2 # no gas input
# Nov 29 = D3 # no endur and titan input
# Nov 30 = D4

# Dec 2 12:28 = HD1
# Dec 3 3:43 = HD2

# =================== Incoming data
#     +------+----------+--------+-------+
#     | date | hist_date| source | value |
#     +------+----------+--------+-------+
#     | D1   | HD1      |endur   | 1     |
#     | D1   | HD1      |power   | 2     |
#     | D1   | HD1      |gas     | 3     |
#     | D1   | HD1      |titan   | 4     |
#     +------+----------+--------+-------+
#     | D1   | HD2      |endur   | 5     |
#     | D1   | HD2      |power   | 6     | #missing gas============== source should solve this, getting the gas from HD1
#     | D1   | HD2      |titan   | 7     |
#     +------+----------+--------+-------+
#     | D2   | HD1      |endur   | 1     |
#     | D2   | HD1      |power   | 2     | #no gas input at all====== this should be delibertly left blank
#     | D2   | HD1      |titan   | 3     |
#     +------+----------+--------+-------+
#     | D2   | HD2      |endur   | 5     |
#     | D2   | HD2      |power   | 6     | #no gas input at all
#     | D2   | HD2      |titan   | 7     |
#     +------+----------+--------+-------+
#     | D3   | HD1      |power   | 1     | # no endur and titan at all
#     | D3   | HD1      |gas     | 2     |
#     +------+----------+--------+-------+
#     | D3   | HD2      |power   | 5     | # no endur and titan at all
#     | D3   | HD2      |gas     | 6     |
#     +------+----------+--------+-------+
#     | D4   | HD1      |endur   | 1     |
#     | D4   | HD1      |power   | 2     |
#     | D4   | HD1      |gas     | 3     |
#     | D4   | HD1      |titan   | 4     |
#     +------+----------+--------+-------+
#     | D4   | HD2      |endur   | 5     |
#     | D4   | HD2      |power   | 6     |
#     | D4   | HD2      |gas     | 7     |
#     | D4   | HD2      |titan   | 8     |
#     +------+----------+--------+-------+

# =================== Existing history
#     +------+----------+--------+-------+
#     | date | hist_date| source | value |
#     +------+----------+--------+-------+
#     | D1   | HD1      |endur   | 1     |
#     | D1   | HD1      |power   | 2     |
#     | D1   | HD1      |gas     | 3     |
#     | D1   | HD1      |titan   | 4     |
#     +------+----------+--------+-------+
#     | D2   | HD1      |endur   | 1     |
#     | D2   | HD1      |power   | 2     |
#     | D2   | HD1      |titan   | 3     |
#     +------+----------+--------+-------+
#     | D3   | HD1      |power   | 1     |
#     | D3   | HD1      |gas     | 2     |
#     +------+----------+--------+-------+
#     | D4   | HD1      |endur   | 1     |
#     | D4   | HD1      |power   | 2     |
#     | D4   | HD1      |gas     | 3     |
#     | D4   | HD1      |titan   | 4     |
#     +------+----------+--------+-------+

# ====================== Expected history
#     +------+----------+--------+-------+
#     | date | hist_date| source | value |
#     +------+----------+--------+-------+
#     | D1   | HD2      |endur   | 5     |
#     | D1   | HD2      |power   | 6     |
#     | D1   | HD1      |gas     | 3     | # thiS should be from HD1
#     | D1   | HD2      |titan   | 7     |
#     +------+----------+--------+-------+
#     | D2   | HD2      |endur   | 5     |
#     | D2   | HD2      |power   | 6     | #no gas input at all
#     | D2   | HD2      |titan   | 7     |
#     +------+----------+--------+-------+
#     | D3   | HD2      |power   | 5     | # no endur and titan at all
#     | D3   | HD2      |gas     | 6     |
#     +------+----------+--------+-------+
#     | D4   | HD2      |endur   | 5     |
#     | D4   | HD2      |power   | 6     |
#     | D4   | HD2      |gas     | 7     |
#     | D4   | HD2      |titan   | 8     |
#     +------+----------+--------+-------+


@pytest.fixture
def edge_case_input():
    schema = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Hist_Datetime', TimestampType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True)
    ])

    incoming_data = [
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
        # ----------------------------------------------------------
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'power', 6),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'titan', 3),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'power', 6),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'power', 1),
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'gas', 2),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'power', 5),
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'gas', 6),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'power', 6),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'gas', 7),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'titan', 8),
    ]

    existing_history_data = [
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'titan', 3),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'power', 1),
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'gas', 2),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
    ]

    expected_history_data = [
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'power', 6),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'power', 6),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'power', 5),
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'gas', 6),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'power', 6),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'gas', 7),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'titan', 8),
    ]
    return (schema, incoming_data, existing_history_data, expected_history_data)


def test_filter_and_replace_common_dates__edge_case(spark_session, edge_case_input):
    schema, incoming_data, existing_history_data, expected_history_data = edge_case_input

    incoming_df = spark_session.createDataFrame(incoming_data, schema)
    existing_history_df = spark_session.createDataFrame(existing_history_data, schema)

    desired_output_df_unsorted = spark_session.createDataFrame(expected_history_data, schema)
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    (output_df_unsorted, mode) = filter_and_replace_common_dates(
        incoming_df, existing_history_df, VALUATION_DATE_COL, SOURCE_COL, HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)

    assert(desired_output_df.schema == output_df.schema)

    assert(mode)

    assert(desired_output_df.count() == output_df.count())

    # assert that output_df == desired_output_df
    assert_frame_equal(desired_output_df.toPandas(), output_df.toPandas())


@pytest.fixture
def schema_drift_input():
    schema_old = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Hist_Datetime', TimestampType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True),
    ])

    schema_new = StructType([
        StructField('Valuation_Date', DateType(), True),
        StructField('Hist_Datetime', TimestampType(), True),
        StructField('Source', StringType(), True),
        StructField('Value', IntegerType(), True),
        StructField('new_col', DoubleType(), True),
    ])

    incoming_data = [
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'power', 2, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'power', 6, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'power', 2, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'titan', 3, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'power', 6, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'power', 1, 0.0),
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'gas', 2, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'power', 5, 0.0),
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'gas', 6, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'power', 2, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'power', 6, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'gas', 7, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'titan', 8, 0.0),
    ]

    existing_history_data = [
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
        # ----------------------------------------------------------
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 28), datetime(2019, 12, 2, 12, 28, 00), 'titan', 3),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'power', 1),
        (date(2019, 11, 29), datetime(2019, 12, 2, 12, 28, 00), 'gas', 2),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'power', 2),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3),
        (date(2019, 11, 30), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4),
    ]

    expected_history_data = [
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'endur', 1, None),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'power', 2, None),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3, None),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 00), 'titan', 4, None),
        # ----------------------------------------------------------
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'power', 6, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 2, 12, 28, 00), 'gas', 3, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'power', 6, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 00), 'titan', 7, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'power', 5, 0.0),
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 00), 'gas', 6, 0.0),
        # ----------------------------------------------------------
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'endur', 5, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'power', 6, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'gas', 7, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 00), 'titan', 8, 0.0),
    ]
    return (schema_old, schema_new, incoming_data, existing_history_data, expected_history_data)


def test_filter_latest_data_schema_drift(spark_session, schema_drift_input):
    schema_old, schema_new, incoming_data, existing_history_data, expected_history_data = schema_drift_input

    incoming_df = spark_session.createDataFrame(incoming_data, schema_new)
    existing_history_df = spark_session.createDataFrame(existing_history_data, schema_old)

    desired_output_df_unsorted = spark_session.createDataFrame(expected_history_data, schema_new)
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    (output_df_unsorted, mode) = filter_and_replace_common_dates(
        incoming_df, existing_history_df, VALUATION_DATE_COL, SOURCE_COL, HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)

    assert(desired_output_df.schema == output_df.schema)

    assert(mode)

    assert(desired_output_df.count() == output_df.count())

    # assert that output_df == desired_output_df
    left = desired_output_df.toPandas()
    right = output_df.toPandas()
    assert_frame_equal(left, right)


@pytest.fixture
def schema_drift_no_source_output():
    # +--------------+-------------------+------+-----+-------+
    # |Valuation_Date|      Hist_Datetime|Source|Value|new_col|
    # +--------------+-------------------+------+-----+-------+
    # |    2019-11-26|2019-12-02 12:28:00| endur|    1|   null|
    # |    2019-11-26|2019-12-02 12:28:00|   gas|    3|   null|
    # |    2019-11-26|2019-12-02 12:28:00| power|    2|   null|
    # |    2019-11-26|2019-12-02 12:28:00| titan|    4|   null|
    # |    2019-11-27|2019-12-03 03:43:00| endur|    5|    0.0|
    # |    2019-11-27|2019-12-03 03:43:00| power|    6|    0.0|
    # |    2019-11-27|2019-12-03 03:43:00| titan|    7|    0.0|
    # |    2019-11-28|2019-12-03 03:43:00| endur|    5|    0.0|
    # |    2019-11-28|2019-12-03 03:43:00| power|    6|    0.0|
    # |    2019-11-28|2019-12-03 03:43:00| titan|    7|    0.0|
    # |    2019-11-29|2019-12-03 03:43:00|   gas|    6|    0.0|
    # |    2019-11-29|2019-12-03 03:43:00| power|    5|    0.0|
    # |    2019-11-30|2019-12-03 03:43:00| endur|    5|    0.0|
    # |    2019-11-30|2019-12-03 03:43:00|   gas|    7|    0.0|
    # |    2019-11-30|2019-12-03 03:43:00| power|    6|    0.0|
    # |    2019-11-30|2019-12-03 03:43:00| titan|    8|    0.0|
    # +--------------+-------------------+------+-----+-------+

    output_data = [
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 0), 'endur', 1, None),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 0), 'gas', 3, None),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 0), 'power', 2, None),
        (date(2019, 11, 26), datetime(2019, 12, 2, 12, 28, 0), 'titan', 4, None),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 0), 'endur', 5, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 0), 'power', 6, 0.0),
        (date(2019, 11, 27), datetime(2019, 12, 3, 3, 43, 0), 'titan', 7, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 0), 'endur', 5, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 0), 'power', 6, 0.0),
        (date(2019, 11, 28), datetime(2019, 12, 3, 3, 43, 0), 'titan', 7, 0.0),
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 0), 'gas', 6, 0.0),
        (date(2019, 11, 29), datetime(2019, 12, 3, 3, 43, 0), 'power', 5, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 0), 'endur', 5, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 0), 'gas', 7, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 0), 'power', 6, 0.0),
        (date(2019, 11, 30), datetime(2019, 12, 3, 3, 43, 0), 'titan', 8, 0.0),
    ]

    return output_data


def test_filter_latest_data_schema_drift_no_source(spark_session, schema_drift_input, schema_drift_no_source_output):
    schema_old, schema_new, incoming_data, existing_history_data, _ = schema_drift_input
    expected_history_data = schema_drift_no_source_output

    incoming_df = spark_session.createDataFrame(incoming_data, schema_new)
    existing_history_df = spark_session.createDataFrame(existing_history_data, schema_old)

    desired_output_df_unsorted = spark_session.createDataFrame(expected_history_data, schema_new)
    desired_output_df = desired_output_df_unsorted.sort(desired_output_df_unsorted.columns)

    (output_df_unsorted, mode) = filter_and_replace_common_dates(
        incoming_df, existing_history_df, VALUATION_DATE_COL, None, HIST_DATETIME_COL)
    output_df = output_df_unsorted.sort(output_df_unsorted.columns)

    assert(desired_output_df.schema == output_df.schema)

    assert(mode)

    assert(desired_output_df.count() == output_df.count())

    # assert that output_df == desired_output_df
    left = desired_output_df.toPandas()
    right = output_df.toPandas()
    assert_frame_equal(left, right)
