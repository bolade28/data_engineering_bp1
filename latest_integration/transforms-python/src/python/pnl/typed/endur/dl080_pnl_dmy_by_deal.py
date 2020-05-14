# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, incremental, Input, Output, configure

# ===== import: our functions
from python.pnl.schema.schema_dl080_pnl_dmy_by_deal import typed_output_schema
from python.bp_flush_control import DL080_PROFILE
from python.util.read_file_data import read_raw_write_typed
from python.util.schema_utils import compute_typed, reorder_columns, is_multiline

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def this_transform(df, typed_output_schema):
    df = compute_typed(df, typed_output_schema)
    df = reorder_columns(df, typed_output_schema)
    return df


@configure(profile=DL080_PROFILE)
@incremental()
@transform(
    transform_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl080_pnl_dmy_by_deal"),
    transform_input=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl080-pnl_dmy_by_deal/dl080-pnl_dmy_by_deal"),
    error_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/errors/dl080_pnl_dmy_by_deal_errors"),
    log_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/log/dl080_pnl_dmy_by_deal_log"),
)
def my_compute_function(transform_input, transform_output, error_output, log_output):
    '''
    Version: V2
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        transform_input (TransformInput)
        transform_output (TransformOutput)
    '''
    read_raw_write_typed(
        transform_input,
        transform_output,
        typed_output_schema,
        is_multiline(transform_input),
        this_transform,
        logger)
