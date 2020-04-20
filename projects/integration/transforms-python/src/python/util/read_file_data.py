# ===== import: python modules
import csv
import sys
import traceback
import cProfile
import io
import pstats

# ===== import: pyspark modules
from pyspark.sql import Row

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


MESSAGE_COLUMN = '_file_reader_message'
FILE_PATH_COLUMN = '_file_reader_file_path'
FILE_SIZE_COLUMN = '_file_reader_file_size'
FILE_MODIFIED_COLUMN = '_file_reader_file_modified'
ROW_TYPE_COLUMN = '_file_reader_row_type'
METADATA_COLUMN_COUNT = 5

ROW_TYPE_DATA = 'data'
ROW_TYPE_HEADER = 'header'
ROW_TYPE_ERROR = 'error'

# Turn this on to write logs that will affect the query plan (e.g. row counts)
# Logs that do not affect the query plan are not controlled.
EXPENSIVE_LOGS = False


def create_spark_row(file_header_columns,
                     schema_columns,
                     file_status,
                     row_type=ROW_TYPE_DATA,
                     csv_row=None,
                     message=''):
    '''
    This function:
        Creates a pyspark.sql.Row object given a CSV row, a list of the columns
        it represents, and a list of expected schema columns.

        It adds file status, row type and message columns for transmission of errors
        and log messages back to the driver.
    '''

    # If the csv_row is None, we need a dummy row containing blanks
    if csv_row is None:
        csv_row = ['' for _ in range(len(file_header_columns))]

    # Make a dictionary from the csv row based on the header
    csv_row_dict = {file_header_columns[i]: csv_row[i] for i in range(len(file_header_columns))}

    # Remove columns not in the schema
    columns_not_in_schema = set(file_header_columns) - set(schema_columns)
    for col in columns_not_in_schema:
        csv_row_dict.pop(col, None)

    # Add columns that are in the schema but not the file
    columns_not_in_file = set(schema_columns) - set(file_header_columns)
    for c in columns_not_in_file:
        csv_row_dict[c] = ''

    # Add message, metdata, columns
    csv_row_dict[FILE_PATH_COLUMN] = file_status.path
    csv_row_dict[FILE_SIZE_COLUMN] = file_status.size
    csv_row_dict[FILE_MODIFIED_COLUMN] = file_status.modified
    csv_row_dict[ROW_TYPE_COLUMN] = str(row_type)
    csv_row_dict[MESSAGE_COLUMN] = message

    return Row(**csv_row_dict)


def read_file_data(transform_input, schema, multi_line):
    '''
    This function:
        Reads all csv files in the current transaction of the TranformInput passed.

        Columns not mentioned in the schema are ignored.

    Arguments:
        transform_input (TransformInput): the TransformInput object passed to the calling transform
        schema: the expected schema of the input data
        multi_line (boolean): a flag which indicates whether the input files are expected to contain multi-line strings

    Returns:
        dataframe: file_content_df - contains the extracted content of the CSV files, plus status information,
        in columns whose names are determined by the *_COLUMN constants above:

            ROW_TYPE_COLUMN (string) = 'data', 'header', 'error' to identify the type of row;
            MESSAGE_COLUMN (string) = for data rows, blank, for header rows, the header, for error rows, the error;
            FILE_PATH_COLUMN (string) = the path of the file this row relates to;
            FILE_SIZE_COLUMN (long) = the size of that file;
            FILE_MODIFIED_COLUMN (long) = the last modified epoch time stamp of that file.

        If data rows are retrieved and not only errors, each row also contains the columns from the schema.
        For error and header rows, the data columns are blank.

        This code uses an RDD flatMap and a generator function to do the file reading.  Although we would
        ideally wish to avoid using these features, we have found empirically that this is the only reliable
        method of reading raw files.
    '''

    def read_file_content(file_status):
        '''
        Nested generator function to extract the file content from the file referenced by its first parameter

        Arguments:
            file_status: a FileStatus object pointing to the file to be read

        Yields:
            Row objects containing the file content and formatted string representations of any
            exceptions encountered while reading.
        '''
        header_columns = []
        try:
            with transform_input.filesystem().open(file_status.path, encoding='cp1252') as f:
                quoting = csv.QUOTE_NONNUMERIC if multi_line else csv.QUOTE_MINIMAL
                r = csv.reader(f, quoting=quoting)

                # Get the header columns
                header_columns = next(r)
                yield create_spark_row(header_columns,
                                       schema_columns,
                                       file_status,
                                       row_type=ROW_TYPE_HEADER,
                                       message=repr(header_columns))

                for row in r:
                    yield create_spark_row(header_columns,
                                           schema_columns,
                                           file_status,
                                           row_type=ROW_TYPE_DATA,
                                           csv_row=row)

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()

            error_message = repr(file_status) + '\n' + \
                repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            yield create_spark_row(header_columns,
                                   schema_columns,
                                   file_status,
                                   row_type=ROW_TYPE_ERROR,
                                   message=error_message)

    files_df = transform_input.filesystem().files('**/*.csv')
    if EXPENSIVE_LOGS:
        logger.info('========= files: {} {}'.format(files_df.schema, files_df.collect()))

    # Get list of column names from the expected schema.
    # This is used as a bound variable in process_file_content above, called from the flatMap below.
    schema_columns = [col.name for col in schema]
    logger.info('========= schema_columns: {}'.format(schema_columns))

    file_content_rdd = files_df.rdd.flatMap(read_file_content)
    if EXPENSIVE_LOGS:
        logger.info('========= file_content_rdd count = {}'.format(file_content_rdd.count()))
        logger.info('========= file_content_rdd contents [:100] = {}'.format(file_content_rdd.take(100)))

    try:
        file_content_df = file_content_rdd.toDF()
        if EXPENSIVE_LOGS:
            logger.info('========= file_content_df row count = {}'.format(file_content_df.count()))
    except ValueError as ve:
        logger.info('========= failed (1) to convert to DataFrame: {}'.format(ve))
        try:
            myRow = Row(FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN, ROW_TYPE_COLUMN, MESSAGE_COLUMN)
            file_content_rdd = \
                file_content_rdd.flatMap(lambda r: [myRow(
                    str(r[FILE_PATH_COLUMN]),
                    str(r[FILE_SIZE_COLUMN]),
                    str(r[FILE_MODIFIED_COLUMN]),
                    str(r[ROW_TYPE_COLUMN]),
                    str(r[MESSAGE_COLUMN]))])
            file_content_df = file_content_rdd.toDF()
        except ValueError:
            logger.info('========= failed (2) to convert to DataFrame: {}'.format(ve))
            file_content_df = None

    return file_content_df


def write_ancillary_outputs(file_content_df, error_output=None, log_output=None):
    if file_content_df:
        if error_output:
            logger.info('========= filtering for errors')
            error_df = file_content_df \
                .filter(file_content_df[ROW_TYPE_COLUMN] == ROW_TYPE_ERROR) \
                .select(FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN, MESSAGE_COLUMN)
            if EXPENSIVE_LOGS:
                logger.info('========= error dataset row count = {}'.format(error_df.count()))
            error_output.set_mode('replace')
            error_output.write_dataframe(error_df)

        if log_output:
            logger.info('========= filtering for headers')
            file_header_df = file_content_df \
                .filter(file_content_df[ROW_TYPE_COLUMN] == ROW_TYPE_HEADER) \
                .select(FILE_PATH_COLUMN, FILE_SIZE_COLUMN, FILE_MODIFIED_COLUMN, MESSAGE_COLUMN)

            if EXPENSIVE_LOGS:
                logger.info('========= log output row count - {}'.format(file_header_df.count()))
            log_output.set_mode('replace')
            log_output.write_dataframe(file_header_df)
    else:
        logger.info('========= no file content retrieved')


def filter_data_rows(file_content_df):
    if file_content_df:
        return file_content_df.filter(file_content_df[ROW_TYPE_COLUMN] == ROW_TYPE_DATA)
    else:
        return None


def read_raw_write_typed(
        transform_input,
        transform_output,
        typed_output_schema,
        multi_line,
        this_transform,
        logger,
        error_output=None,
        log_output=None,
        ):

    ENABLE_PROFILE = False

    if ENABLE_PROFILE:
        pr = cProfile.Profile()
        pr.enable()
        file_content_df = pr.runcall(read_file_data, transform_input, typed_output_schema, multi_line=multi_line)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats()
        logger.info('========= profile {}'.format(s.getvalue()))
    else:
        file_content_df = read_file_data(transform_input,
                                         typed_output_schema,
                                         multi_line=multi_line)

    if file_content_df:
        # If there are more than just the metadata columns, there is data to write
        if len(file_content_df.columns) > METADATA_COLUMN_COUNT:
            typed_df = this_transform(filter_data_rows(file_content_df), typed_output_schema)
            if EXPENSIVE_LOGS:
                logger.info('========= data to write, row count = {}'.format(typed_df.count()))
            transform_output.write_dataframe(typed_df)

    write_ancillary_outputs(file_content_df, error_output, log_output)
