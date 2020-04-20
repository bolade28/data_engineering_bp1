import errno
import os
from unittest.mock import Mock
from contextlib import contextmanager
from pandas.testing import assert_frame_equal
from pytest import fixture
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType

from transforms.api import FileStatus

from python.util.read_file_data import \
    create_spark_row, read_file_data, filter_data_rows, write_ancillary_outputs, \
    ROW_TYPE_COLUMN, MESSAGE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN, \
    ROW_TYPE_DATA, ROW_TYPE_HEADER, ROW_TYPE_ERROR


@fixture
def row_filters_input_data_schema():
    return StructType([
        StructField('A', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])


@fixture
def row_filters_input_data():
    return [
        Row(A='', _file_reader_row_type=ROW_TYPE_HEADER, _file_reader_message="['A']", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
        Row(A='1', _file_reader_row_type=ROW_TYPE_DATA, _file_reader_message="", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
        Row(A='', _file_reader_row_type=ROW_TYPE_ERROR, _file_reader_message="Error message", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
    ]


def test_write_ancillary_outputs(
        spark_session,
        row_filters_input_data_schema,
        row_filters_input_data):

    error_output_data = [
        Row(_file_reader_message="Error message", _file_reader_file_path='', _file_reader_file_size=0,
            _file_reader_file_modified=0),
    ]

    log_output_data = [
        Row(_file_reader_message="['A']", _file_reader_file_path='', _file_reader_file_size=0,
            _file_reader_file_modified=0),
    ]

    expected_output_schema = StructType([
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    error_output = Mock()
    log_output = Mock()

    input_df = spark_session.createDataFrame(row_filters_input_data, row_filters_input_data_schema)
    expected_error_output_df = spark_session.createDataFrame(error_output_data, expected_output_schema)
    expected_log_output_df = spark_session.createDataFrame(log_output_data, expected_output_schema)

    write_ancillary_outputs(input_df, error_output, log_output)

    error_output.set_mode.assert_called_once_with('replace')
    error_output.write_dataframe.assert_called_once()
    assert(len(error_output.write_dataframe.call_args[0]) == 1)
    error_output_df = error_output.write_dataframe.call_args[0][0]
    assert_frame_equal(error_output_df.toPandas(), expected_error_output_df.toPandas(), check_like=True)

    log_output.set_mode.assert_called_once_with('replace')
    log_output.write_dataframe.assert_called_once()
    assert(len(log_output.write_dataframe.call_args[0]) == 1)
    log_output_df = log_output.write_dataframe.call_args[0][0]
    assert_frame_equal(log_output_df.toPandas(), expected_log_output_df.toPandas(), check_like=True)


def test_filter_data_rows(spark_session):
    schema = StructType([
        StructField('A', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),

    ])

    # Input data contains rows of type data, header and error
    input_data = [
        Row(A='', _file_reader_row_type=ROW_TYPE_HEADER, _file_reader_message="['A']", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
        Row(A='1', _file_reader_row_type=ROW_TYPE_DATA, _file_reader_message="", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
        Row(A='', _file_reader_row_type=ROW_TYPE_ERROR, _file_reader_message="Error message", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
    ]

    # Expected output data contains row of type data only, after filtering for data rows
    expected_output_data = [
        Row(A='1', _file_reader_row_type=ROW_TYPE_DATA, _file_reader_message="", _file_reader_file_path='',
            _file_reader_file_size=0, _file_reader_file_modified=0),
    ]
    input_df = spark_session.createDataFrame(input_data, schema)
    actual_output_df = filter_data_rows(input_df)
    expected_output_df = spark_session.createDataFrame(expected_output_data, schema)
    assert(actual_output_df.schema == schema)
    assert_frame_equal(expected_output_df.toPandas(), actual_output_df.toPandas())


def test_filter_data_rows_null():
    output_df = filter_data_rows(None)
    assert(output_df is None)


def test_create_spark_row_no_error():

    file_header_columns = ['col1', 'col2']
    schema_columns = ['col1', 'col2']
    csv_row = ['1', '2']
    file_status = FileStatus(path='file1', size=100, modified=1)

    actual = create_spark_row(file_header_columns, schema_columns, file_status, csv_row=csv_row)
    expected = Row(
            col1='1',
            col2='2',
            _file_reader_row_type='data',
            _file_reader_message='',
            _file_reader_file_path=file_status.path,
            _file_reader_file_modified=file_status.modified,
            _file_reader_file_size=file_status.size)

    assert(actual == expected)


def test_create_spark_row_error():

    file_header_columns = []
    schema_columns = ['col1', 'col2']
    error_message = 'Error Message'
    file_status = FileStatus(path='file1', size=100, modified=1)

    actual = create_spark_row(
        file_header_columns, schema_columns, file_status,
        row_type='error', message=error_message)

    expected = Row(
            col1='',
            col2='',
            _file_reader_row_type='error',
            _file_reader_message=error_message,
            _file_reader_file_path=file_status.path,
            _file_reader_file_modified=file_status.modified,
            _file_reader_file_size=file_status.size)

    assert(actual == expected)


def test_create_spark_row_extra_column():

    file_header_columns = ['A', 'B']
    schema_columns = ['A']
    csv_row = ['1', '2']

    file_status = FileStatus(path='file1', size=100, modified=1)

    actual = create_spark_row(file_header_columns, schema_columns, file_status, csv_row=csv_row)
    expected = Row(
            A='1',
            _file_reader_row_type='data',
            _file_reader_message='',
            _file_reader_file_path=file_status.path,
            _file_reader_file_modified=file_status.modified,
            _file_reader_file_size=file_status.size)

    assert(actual == expected)


def test_create_spark_row_missing_column():
    file_header_columns = ['A']
    schema_columns = ['A', 'B']
    csv_row = ['1']

    file_status = FileStatus(path='file1', size=100, modified=1)

    actual = create_spark_row(file_header_columns, schema_columns, file_status, csv_row=csv_row)
    expected = Row(
            A='1',
            B='',
            _file_reader_row_type='data',
            _file_reader_message='',
            _file_reader_file_path=file_status.path,
            _file_reader_file_modified=file_status.modified,
            _file_reader_file_size=file_status.size)

    assert(actual == expected)


def test_read_file_data(spark_session):
    mock_FileStatus_list = [
        FileStatus(path='file1', size=100, modified=1),
        FileStatus(path='file2', size=200, modified=2),
        FileStatus(path='file3', size=300, modified=3),
    ]

    input_file_data = {
        'file1': '"A","B","C"\n"1","2","3"\n',
        'file2': '"A","B","C"\n"2","3","4"\n',
        'file3': '"A","B","C"\n"3","4","5"\n',
    }

    protoRow = Row('A', 'B', 'C',
                   ROW_TYPE_COLUMN, MESSAGE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN)

    expected_file_content = [
        protoRow(
            '', '', '', 'header', "['A', 'B', 'C']",
            mock_FileStatus_list[0].path, mock_FileStatus_list[0].size, mock_FileStatus_list[0].modified),
        protoRow(
            '1', '2', '3', 'data', '',
            mock_FileStatus_list[0].path, mock_FileStatus_list[0].size, mock_FileStatus_list[0].modified),
        protoRow(
            '', '', '', 'header', "['A', 'B', 'C']",
            mock_FileStatus_list[1].path, mock_FileStatus_list[1].size, mock_FileStatus_list[1].modified),
        protoRow(
            '2', '3', '4', 'data', '',
            mock_FileStatus_list[1].path, mock_FileStatus_list[1].size, mock_FileStatus_list[1].modified),
        protoRow(
            '', '', '', 'header', "['A', 'B', 'C']",
            mock_FileStatus_list[2].path, mock_FileStatus_list[2].size, mock_FileStatus_list[2].modified),
        protoRow(
            '3', '4', '5', 'data', '',
            mock_FileStatus_list[2].path, mock_FileStatus_list[2].size, mock_FileStatus_list[2].modified),
    ]

    expected_file_schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
        StructField('C', StringType()),
    ])

    expected_return_schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
        StructField('C', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    mock_transform_input = construct_mock_TransformInput(spark_session, mock_FileStatus_list, input_file_data)

    multi_line = False

    file_content_df = read_file_data(mock_transform_input, expected_file_schema, multi_line)

    expected_df = spark_session.createDataFrame(expected_file_content, expected_return_schema)
    file_content_pandas = file_content_df.toPandas()
    expected_pandas = expected_df.toPandas()
    assert_frame_equal(file_content_pandas, expected_pandas, check_like=True)


def test_read_multiline_file_data(spark_session):
    FileStatus_list = [
        FileStatus(path='file1', size=100, modified=1),
        FileStatus(path='file2', size=200, modified=2),
    ]

    multiline_input_data = {
        'file1': '"A","B"\n"First half\nSecond half","1"\n"Next line","2"\n',
        'file2': '"A","B"\n"Second file","3"\n',
    }

    protoRow = Row('A', 'B', ROW_TYPE_COLUMN, MESSAGE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN)

    expected_multline_content = [
        protoRow(
            '', '', 'header', "['A', 'B']",
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            'First halfSecond half', '1', 'data', '',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            'Next line', '2', 'data', '',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            '', '', 'header', "['A', 'B']",
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
        protoRow(
            'Second file', '3', 'data', '',
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
    ]

    expected_file_schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
    ])

    expected_return_schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    mock_transform_input = construct_mock_TransformInput(spark_session, FileStatus_list, multiline_input_data)

    multi_line = True

    file_content_df = read_file_data(mock_transform_input, expected_file_schema, multi_line)
    expected_df = spark_session.createDataFrame(expected_multline_content, expected_return_schema)
    file_content_pandas = file_content_df.toPandas()
    expected_pandas = expected_df.toPandas()
    assert_frame_equal(file_content_pandas, expected_pandas, check_like=True)


def test_read_file_extra_column(spark_session):
    '''
    This test simulates reading a file with a column not mentioned in the schema
    '''
    FileStatus_list = [
        FileStatus(path='file1', size=100, modified=1),
        FileStatus(path='file2', size=200, modified=2),
    ]

    multiline_input_data = {
        'file1': '"A","B"\n"First file","1"\n"Next line","2"\n',
        'file2': '"A"\n"Second file"\n',
    }

    protoRow = Row('A', ROW_TYPE_COLUMN, MESSAGE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN)

    expected_content = [
        protoRow(
            '', 'header', "['A', 'B']",
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            'First file', 'data', '',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            'Next line', 'data', '',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            '', 'header', "['A']",
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
        protoRow(
            'Second file', 'data', '',
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
    ]

    expected_file_schema = StructType([
        StructField('A', StringType()),
    ])

    expected_return_schema = StructType([
        StructField('A', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    mock_transform_input = construct_mock_TransformInput(spark_session, FileStatus_list, multiline_input_data)

    file_content_df = read_file_data(mock_transform_input, expected_file_schema, False)

    expected_df = spark_session.createDataFrame(expected_content, expected_return_schema)
    file_content_pandas = file_content_df.toPandas()
    expected_pandas = expected_df.toPandas()
    assert_frame_equal(file_content_pandas, expected_pandas, check_like=True)


def test_read_file_missing_column(spark_session):
    '''
    This test simulates reading a file with a column not mentioned in the schema
    '''
    FileStatus_list = [
        FileStatus(path='file1', size=100, modified=1),
        FileStatus(path='file2', size=200, modified=2),
    ]

    multiline_input_data = {
        'file1': '"A","B","C"\n"First file","1","1.1"\n"Next line","2","2.2"\n',
        'file2': '"A","C"\n"Second file","3.3"\n',
    }

    protoRow = Row(
        'A', 'B', 'C', ROW_TYPE_COLUMN, MESSAGE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN)

    expected_content = [
        protoRow(
            '', '', '', 'header', "['A', 'B', 'C']",
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            'First file', '1', '1.1', 'data', '',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            'Next line', '2', '2.2', 'data', '',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            '', '', '', 'header', "['A', 'C']",
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
        protoRow(
            'Second file', '', '3.3', 'data', '',
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
    ]

    expected_file_schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
        StructField('C', StringType()),
    ])

    expected_return_schema = StructType([
        StructField('A', StringType()),
        StructField('B', StringType()),
        StructField('C', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(MESSAGE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    mock_transform_input = construct_mock_TransformInput(spark_session, FileStatus_list, multiline_input_data)

    file_content_df = read_file_data(mock_transform_input, expected_file_schema, False)
    expected_df = spark_session.createDataFrame(expected_content, expected_return_schema)
    file_content_pandas = file_content_df.toPandas()
    expected_pandas = expected_df.toPandas()
    assert_frame_equal(file_content_pandas, expected_pandas, check_like=True)


def test_file_open_exception(spark_session):
    '''
    This test simulates an exception being thrown while opening a file
    '''
    FileStatus_list = [
        FileStatus(path='file1', size=100, modified=1),
        FileStatus(path='file2', size=200, modified=2),
    ]
    input_data = {
        'file1': '"A"\n"First file"\n',
        'file2': '"A"\n"Second file","3"\n',
    }

    expected_file_schema = StructType([
        StructField('A', StringType()),
    ])

    # Note: error column missing here because we don't care about its contents for the
    # purposes of comparing DataFrames
    protoRow = Row('A', ROW_TYPE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN)

    expected_content = [
        protoRow(
            '', 'error',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            '', 'error',
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
    ]

    expected_return_schema = StructType([
        StructField('A', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    mock_transform_input = construct_mock_TransformInput(
        spark_session,
        FileStatus_list,
        input_data,
        throw_exception_open=True)

    file_content_df = read_file_data(mock_transform_input, expected_file_schema, False)

    assert(MESSAGE_COLUMN in file_content_df.columns)

    messages = file_content_df.select(MESSAGE_COLUMN).collect()
    for m in messages:
        assert('FileNotFoundError' in m[MESSAGE_COLUMN])

    expected_df = spark_session.createDataFrame(expected_content, expected_return_schema)
    file_content_pandas = file_content_df.select(
        'A', ROW_TYPE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN).toPandas()
    expected_pandas = expected_df.toPandas()
    assert_frame_equal(file_content_pandas, expected_pandas, check_like=True)


def disabled__test_file_read_exception(spark_session):
    '''
    This test simulates an exception being thrown while opening a file
    '''
    FileStatus_list = [
        FileStatus(path='file1', size=100, modified=1),
        FileStatus(path='file2', size=200, modified=2),
    ]
    input_data = {
        'file1': '"A"\n"First file"\n',
        'file2': '"A"\n"Second file","3"\n',
    }

    expected_file_schema = StructType([
        StructField('A', StringType()),
    ])

    # Note: error column missing here because we don't care about its contents
    protoRow = Row('A', ROW_TYPE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN)

    expected_content = [
        protoRow(
            '', 'header',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            '', 'error',
            FileStatus_list[0].path, FileStatus_list[0].size, FileStatus_list[0].modified),
        protoRow(
            '', 'header',
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
        protoRow(
            '', 'error',
            FileStatus_list[1].path, FileStatus_list[1].size, FileStatus_list[1].modified),
    ]

    expected_return_schema = StructType([
        StructField('A', StringType()),
        StructField(ROW_TYPE_COLUMN, StringType()),
        StructField(FILE_PATH_COLUMN, StringType()),
        StructField(FILE_SIZE_COLUMN, LongType()),
        StructField(FILE_MODIFIED_COLUMN, LongType()),
    ])

    mock_transform_input = construct_mock_TransformInput(
        spark_session,
        FileStatus_list,
        input_data,
        throw_exception_after_lines=1)

    file_content_df = read_file_data(mock_transform_input, expected_file_schema, False)

    assert(MESSAGE_COLUMN in file_content_df.columns)

    expected_df = spark_session.createDataFrame(expected_content, expected_return_schema)
    file_content_pandas = file_content_df.select(
        'A', ROW_TYPE_COLUMN, FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN).toPandas()
    expected_pandas = expected_df.toPandas()
    assert_frame_equal(file_content_pandas, expected_pandas, check_like=True)
    assert(False)

##################################################################################################
#
# Functions to construct and modify mock objects used in the tests
#
# Be cautious about modifying these!


def construct_mock_TransformInput(ss,
                                  file_status_list,
                                  file_data,
                                  throw_exception_open=False,
                                  throw_exception_after_lines=None):
    mock_TransformInput = Mock()
    mock_TransformInput.filesystem.return_value = construct_mock_FileSystem(
        ss,
        file_status_list,
        file_data,
        throw_exception_open,
        throw_exception_after_lines)
    return mock_TransformInput


def construct_mock_FileSystem(ss, file_status_list, file_data, throw_exception_open, throw_exception_after_lines):
    mock_FileSystem = Mock()
    mock_FileSystem.files.return_value = construct_mock_files_DataFrame(
        ss, file_status_list, throw_exception_after_lines)
    mock_FileSystem.open.side_effect = mock_open_file(file_status_list, file_data, throw_exception_open)
    return mock_FileSystem


def construct_mock_files_DataFrame(ss, file_status_list, throw_exception_after_lines):
    mock_df = Mock()
    mock_files_rdd = Mock()
    mock_df.rdd = mock_files_rdd
    mock_files_rdd._content = file_status_list
    mock_content_rdd = Mock()
    mock_content_rdd._content = []
    mock_content_rdd.toDF = mock_toDF(mock_content_rdd, ss)
    mock_result_rdd = Mock()
    mock_result_rdd._content = []
    mock_result_rdd.toDF = mock_toDF(mock_content_rdd, ss)
    mock_files_rdd.flatMap = mock_flatMap(mock_files_rdd, mock_content_rdd)
    mock_content_rdd.flatMap = mock_flatMap(mock_content_rdd, mock_result_rdd, throw_exception_after_lines)
    return mock_df


# Wanted to use mock_open here but it doesn't support iteration in python 3.6
def mock_open_file(file_status_list, file_data, throw_exception_open):
    @contextmanager
    def _inner(path, encoding='cp1252'):
        if (path not in [x for (x, _, _) in file_status_list]) | throw_exception_open:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        assert(encoding == 'cp1252')
        file_text = file_data[path]
        yield file_text.splitlines()
    return _inner


def mock_flatMap(not_self, mock_output_rdd, throw_exception_after_lines=None):
    def _inner(f):
        result = [f(x) for x in not_self._content]
        for l in result:
            line = 1
            for x in l:
                line = line + 1
                if throw_exception_after_lines and (line > throw_exception_after_lines):
                    raise BrokenPipeError(errno.EPIPE, os.strerror(errno.EPIPE), '')
                mock_output_rdd._content.append(x)
        return mock_output_rdd
    return _inner


def mock_toDF(not_self, ss):
    return lambda: ss.createDataFrame(not_self._content)
