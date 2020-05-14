from pyspark.sql.types import StructType, StructField, StringType

import pyspark.sql.functions as f
import logging

logger = logging.getLogger(__name__)


def build_input_data(ctx, input_data, prev_col_names, expected_fields):

    # create spark session and spark context
    ss = ctx.spark_session

    # Determine the existing schema
    logger.info("prev_col_names: "+str(prev_col_names))

    validate = True

    # get object representing the filesystem of the input dataset
    fs = input_data.filesystem()
    raw_underlying_files = fs.files()
    ls_result = fs.ls()
    for fls in ls_result:
        logger.info("===== DEVELOPER fs: {}".format(fls.path))

    logger.info("=== raw_not empty: {}".format(raw_underlying_files.count() > 0))

    if raw_underlying_files.count() > 0:
        # create base file path of underlying files based on filesystem path
        base_file_path = (fs.hadoop_path + '/')

        logger.info("==== base path: {}".format(base_file_path))

        # get list of objects representing each file underlying the input dataset
        filename_list = [r['path'] for r in raw_underlying_files.collect()]

        logger.info("===== filename_list: {}".format(filename_list))

        all_col_names = set(prev_col_names)
        dfArray = []

        for filename in filename_list:
            logger.info("Reading " + filename)
            full_file_path = base_file_path + filename
            curr_df = ss.read.format('csv').option('header', 'true').option(
                'multiLine', 'true').option('delimiter', ",").load(full_file_path)
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
