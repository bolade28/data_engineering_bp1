# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure
# ===== import: our functions
from python.pnl.schema.schema_devops_notification import raw_out_schema_reval, raw_out_schema_820
from python.util.read_file_data import read_file_data, filter_data_rows
from python.util.schema_utils import is_multiline, compute_typed
from python.util.bp_devops import identify_processed_dataset

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def this_transform(raw_df, schema):
    if raw_df is None:
        return None

    logger.info("========= raw_df: {}".format(repr(raw_df.take(100))))
    typed_df = compute_typed(raw_df, schema)
    logger.info("typed_df: {}".format(repr(typed_df.take(100))))
    return typed_df


def calc_values(raw_df, typed_df, schema, dataset_name):
    file_content_df = read_file_data(
        raw_df,
        schema=schema,
        multi_line=is_multiline(raw_df))

    data_df = filter_data_rows(file_content_df)
    output_df = this_transform(data_df, schema)
    type_df = typed_df.dataframe()
    df = identify_processed_dataset(output_df, type_df, dataset_name, "Reval_Date")
    return df


@configure(profile=['NUM_EXECUTORS_16'])
@incremental(snapshot_inputs=['dl800_typed', 'dl010_typed', 'dl820_typed', 'dl020_typed'])
@transform(
    transform_output=Output("/BP/IST-IG-DD/data/technical/devops/notification/notification_without_schema"),
    dl800_typed=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl800_new_amended_cancelled_deals"),
    dl800_raw=Input(
        "/BP/IST-IG-SS-Systems/data/raw/endur/dl800-new_amended_cancelled_deals/dl800-new_amended_cancelled_deals"),
    dl820_typed=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl820-trade_listing"),
    dl820_raw=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl820-trade_listing/dl820-trade_listing"),
    dl010_typed=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl010_pnl_detail"),
    dl010_raw=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl010-pnl_detail/dl010_pnl_detail"),
    dl020_typed=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl020-pnl_detail_mtd"),
    dl020_raw=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl020-pnl_detail_mtd/dl020-pnl_detail_mtd"),
    )
def dtd_calc(dl800_typed, transform_output, dl800_raw, dl820_typed, dl820_raw, dl010_typed, dl010_raw,
             dl020_typed, dl020_raw):
    """ Transformation stage function
    This function:
        Version: T1
        Reads the distinct value of reval_date from input raw and transformed
        then write into a new data set with differences

    Args:
        dl050_input (TransformInput): typed dataset
        transform_input (TransformInput): Raw new file dataset
        transform_output (TransformOutput): Output of the transform

    Returns:
        dataframe: old dataset
    """
    # Dl 800
    df = calc_values(dl800_raw, dl800_typed, raw_out_schema_reval, 'DL800')

    # this will read 820 and union to the above dataset

    df_820 = calc_values(dl820_raw, dl820_typed, raw_out_schema_820, 'DL820')
    df = df.union(df_820)

    # this will read 010 and union to the above dataset

    df_010 = calc_values(dl010_raw, dl010_typed, raw_out_schema_reval, 'DL010')
    df = df.union(df_010)

    # this will read 020 and union to the above dataset

    df_020 = calc_values(dl020_raw, dl020_typed, raw_out_schema_reval, 'DL020')
    df = df.union(df_020)
    transform_output.write_dataframe(df)
