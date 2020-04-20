import logging
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, LongType
from pyspark.sql import Row

from transforms.api import transform, Input, Output, configure

from python.util.read_file_data import read_file_data, ROW_TYPE_COLUMN
from python.util.schema_utils import get_typed_output_schema, is_multiline

logger = logging.getLogger(__name__)

# Please note that, although this file contains tests, it is deliberately named so
# that it is not within Code Repositories' test file naming convention, because it
# runs as a transform, not a pytest test suite.  This is necessary to test the
# correct behaviour of the file reading functions on real datasets.

output_schema = StructType([
    StructField('test_name', StringType(), True),
    StructField('actual', StringType(), True),
    StructField('expected', StringType(), True),
    StructField('success', BooleanType(), True)
])


@configure(profile=['NUM_EXECUTORS_16'])
@transform(
    transform_results_output=Output("/BP/IST-IG-DD/technical/TestFixtures/FileReading/test_results"),
    transform_generic_output=Output("/BP/IST-IG-DD/technical/TestFixtures/FileReading/output"),
    single_file_input=Input("/BP/IST-IG-DD/technical/TestFixtures/FileReading/single_file"),
    multi_file_schema_drift_input=Input("/BP/IST-IG-DD/technical/TestFixtures/FileReading/multi_file_schema_drift"),
    multi_line_input=Input("/BP/IST-IG-DD/technical/TestFixtures/FileReading/multi_line_file"),
    dl080_input=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl080-pnl_dmy_by_deal/dl080-pnl_dmy_by_deal"),
    tr160_input=Input("/BP/IST-IG-SS-Systems/data/raw/titan/tr160-pnl_output/tr160-pnl_output"),
)
def my_compute_function(
                        ctx,
                        single_file_input,
                        multi_file_schema_drift_input,
                        multi_line_input,
                        transform_results_output,
                        transform_generic_output,
                        dl080_input,
                        tr160_input,
                        ):

    test_functions = [
        (transform_test_get_multi_line_content, multi_line_input, transform_generic_output),
        (transform_test_read_dataset, dl080_input, transform_generic_output),
        (transform_test_read_dataset, tr160_input, transform_generic_output),
        (transform_test_schema_drift, multi_file_schema_drift_input, transform_generic_output),
    ]

    output = [f(ctx, transform_input, transform_output) for (f, transform_input, transform_output) in test_functions]
    output_flattened = [val for sublist in output for val in sublist]

    output_df = ctx.spark_session.createDataFrame(data=output_flattened, schema=output_schema)
    transform_results_output.write_dataframe(output_df)


def transform_test_get_multi_line_content(ctx, transform_input, transform_output):
    schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
        StructField('C', StringType()),
    ])
    actual = read_file_data(transform_input, schema, multi_line=True)
    actual_data = actual.filter(actual[ROW_TYPE_COLUMN] == 'data').select('A', 'B', 'C').collect()
    expected_data = [
        Row(u'one line', u'one line', u'one line'),
        Row(u'two \nlines', u'two \nlines', u'two \nlines')
    ]

    return [
        ('test_get_file_content_data', repr(actual_data), repr(expected_data), actual_data == expected_data)
    ]


def transform_test_read_dataset(ctx, transform_input, transform_output):
    schema = get_typed_output_schema(transform_input)
    file_content_df = read_file_data(transform_input, schema, multi_line=is_multiline(transform_input))

    logger.info("========= count = " + str(file_content_df.count()))
    return [
        ('transform_test_read_dataset', str(file_content_df.take(10)), '', True),
        ('transform_test_read_dataset', str(file_content_df.count()), '', True),
    ]


def transform_test_schema_drift(ctx, transform_input, transform_output):
    schema = StructType([
        StructField('A', StringType(), True),
        StructField('B', StringType(), True),
        StructField('C', StringType(), True),
        StructField('D', StringType(), True),
        StructField('E', StringType(), True),
        StructField('_file_reader_file_modified', LongType(), True),
        StructField('_file_reader_file_path', StringType(), True),
        StructField('_file_reader_file_size', LongType(), True),
        StructField('_file_reader_message', StringType(), True),
        StructField('_file_reader_row_type', StringType(), True),
    ])

    df_result = read_file_data(transform_input, schema, False)
    return [
        ('schema drift rows', str(df_result.count()), '6', df_result.count() == 6),
        ('schema drift schema', repr(df_result.schema), repr(schema), df_result.schema == schema),
        ('schema drift', str(df_result.take(10)), '', True),
    ]
