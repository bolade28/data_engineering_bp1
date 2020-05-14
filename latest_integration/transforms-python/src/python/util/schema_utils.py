# ===== import: python function
import pyspark.sql.functions as f
from importlib import import_module

# ===== import: palantir functions

# ===== import: our functions
from python.util.bp_constants import MAP_COLUMN_FORMAT, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


'''
This module:


Todo:
    Convention = ticket_number, description, date_last_updated

'''
_schema_modules = {
    '/BP/IST-IG-SS-Systems/data/raw/endur/dl080-pnl_dmy_by_deal/dl080-pnl_dmy_by_deal':
        'python.pnl.schema.schema_dl080_pnl_dmy_by_deal',
    '/BP/IST-IG-SS-Systems/data/raw/titan/tr160-pnl_output/tr160-pnl_output':
        'python.pnl.schema.schema_tr160_pnl_output',
    '/BP/IST-IG-SS-Systems/data/raw/titan/tr200_exposure/tr200_exposure':
        'python.exposure.schema_exposure.schema_tr200_exposure_combined_detail',
    '/BP/IST-IG-SS-Systems/data/raw/endur/dl800-new_amended_cancelled_deals/dl800-new_amended_cancelled_deals':
        'python.pnl.schema.schema_dl800_new_amended_cancelled_deals'
}

_multiline_datasets = [
    '/BP/IST-IG-SS-Systems/data/raw/endur/dl020-pnl_detail_mtd'
]


def is_multiline(transform_input):
    return transform_input.path in _multiline_datasets


def get_typed_output_schema(transform_input):
    module_path = _schema_modules[transform_input.path]
    schema_module = import_module(module_path)
    return schema_module.typed_output_schema


def compute_typed(df, schema):
    '''
    This function:



    '''
    df_out = df
    logger.info("======= DEVELOPER compute_typed STARTED. ========")
    for row in schema:
        literals = row.metadata.get("default")
        if literals:
            df_out = df_out.withColumn(row.name, f.lit(literals))
        elif str(row.dataType) == "DateType":
            iformat = row.metadata.get("dt_format") if row.metadata.get("dt_format") else DEFAULT_DATE_FORMAT
            df_out = df_out.withColumn(row.name, f.to_date(df[row.name], iformat).alias(row.name))
        elif str(row.dataType) == "TimestampType":
            iformat = row.metadata.get("tst_format") if row.metadata.get("tst_format") else DEFAULT_TIMESTAMP_FORMAT
            df_out = df_out.withColumn(row.name, f.to_timestamp(df[row.name], iformat).alias(row.name))
        elif str(row.dataType) in ["IntegerType", "FloatType", "DoubleType"]:
            df_out = df_out.withColumn(row.name, df[row.name].cast(MAP_COLUMN_FORMAT[str(row.dataType)]))

    logger.info("======== DEVELOPER compute_typed FINISHED ======")
    return df_out


def compute_transform(df, schema):
    '''
    This function:


    '''
    df_out = df
    for row in schema:
        old = row.metadata.get("from")
        if old:
            df_out = df_out.withColumnRenamed(old, row.name)

    return df_out


def reorder_columns(df, schema):
    '''
    This function:


    '''
    expected_fields = list(map(lambda x: x.name, schema))
    return df.select(*expected_fields)
