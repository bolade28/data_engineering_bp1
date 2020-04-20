# ===== import: python function
import pyspark.sql.functions as f
from pyspark.sql.types import Row, StructType, StructField, StringType
import csv

# ===== import: palantir functions

# ===== import: our functions

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def get_file_metadata(transform_input):
    fs = transform_input.filesystem()
    files_df = fs.files()
    files = files_df.collect()
    return [r['path'] for r in files]


def get_file_content(ss, transform_input, path, multi_line=False):
    data_list = []
    with transform_input.filesystem().open(path, encoding='cp1252') as f:
        if multi_line:
            reader = csv.reader(f, quoting=csv.QUOTE_NONNUMERIC)
        else:
            reader = csv.reader(f)

        header = next(reader)
        myRow = Row(*header)

        for row in reader:
            data_list.append(myRow(*row))
    return ss.createDataFrame(data_list)


def build_input_data(ss, transform_input, transform_output, output_schema, flag):
    '''
        This function is used in the typed stage of the pipeline to retrieve the raw data.
        It relies on the Transforms API to provide a transaction containing the relevant files.
        It accepts dependency injection via the *_f arguments to make it testable.

        Args:
            ss: Spark session
            transform_output: the TransformOutput object for the transform in progress
            expected_fields: a list of the columns names expected to exist in the output

        Returns:
            A dataframe containing the data from all input files.
    '''

    # TODO: check types of transform_input and transform_output

    # Determine the existing schema
    try:
        prev_col_names = transform_output.dataframe().columns
    except Exception:
        prev_col_names = []

    expected_fields = list(map(lambda x: x.name, output_schema))

    logger.info("prev_col_names: "+str(prev_col_names))

    validate = True

    raw_underlying_files = get_file_metadata(transform_input)

    if len(raw_underlying_files) > 0:

        all_col_names = set(prev_col_names)
        dfArray = []

        for full_file_path in raw_underlying_files:
            logger.info("Reading " + full_file_path)
            curr_df = get_file_content(ss, transform_input, full_file_path)
            col_names = curr_df.columns
            all_col_names.update(col_names)
            dfArray.append(curr_df)

        logger.info("all_col_names: "+str(all_col_names))

        masterDFComputed = []
        for df in dfArray:
            logger.info("processing dataframe")
            for col in all_col_names:
                if col not in df.columns:
                    logger.info("adding col "+col)
                    df = df.withColumn(col, f.lit(None))
            masterDFComputed.append(df)

        logger.info("len(masterDFComputed): "+str(len(masterDFComputed)))

        all_col_names = sorted(all_col_names)
        if validate:
            full_schema = StructType([StructField(c, StringType(), True) for c in sorted(expected_fields)])
        else:
            full_schema = StructType([StructField(c, StringType(), True) for c in sorted(all_col_names)])

        master_df = ss.createDataFrame(data=[], schema=full_schema)

        # masterDFSorted = []
        for calc_df in masterDFComputed:
            calc_df = calc_df.select(all_col_names)
            master_df = master_df.union(calc_df)

        return master_df
