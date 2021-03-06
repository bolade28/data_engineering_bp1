# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, incremental, Input, Output, configure

# ===== import: our functions
from python.pnl.schema.schema_dl020_pnl_detail_mtd import typed_output_schema
from python.bp_flush_control import DL020_PROFILE
from python.util.read_file_data import read_raw_write_typed
from python.util.schema_utils import compute_typed, reorder_columns, is_multiline

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def this_transform(df, typed_output_schema):
    df = compute_typed(df, typed_output_schema)
    df = reorder_columns(df, typed_output_schema)
    return df


@configure(profile=DL020_PROFILE)
@incremental()
@transform(
    transform_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl020-pnl_detail_mtd"),
    transform_input=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl020-pnl_detail_mtd/dl020-pnl_detail_mtd"),
    error_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/errors/dl020_pnl_detail_mtd_errors"),
    log_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/log/dl020_pnl_detail_mtd_log"),
)
def my_compute_function(transform_input, transform_output, error_output, log_output):
    '''
    Version: V0 R1
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
