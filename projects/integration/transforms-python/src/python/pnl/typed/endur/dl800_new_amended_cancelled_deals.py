# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, incremental, Input, Output, configure

# ===== import: our functions
from python.pnl.schema.schema_dl800_new_amended_cancelled_deals import typed_output_schema
from python.bp_flush_control import DL800_PROFILE
from python.util.read_file_data import read_raw_write_typed
from python.util.schema_utils import compute_typed, reorder_columns, is_multiline

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def this_transform(df, typed_output_schema):
    df = compute_typed(df, typed_output_schema)
    df = reorder_columns(df, typed_output_schema)
    return df


@configure(profile=DL800_PROFILE)
@incremental()
@transform(
    transform_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl800_new_amended_cancelled_deals"),
    transform_input=Input(
        "/BP/IST-IG-SS-Systems/data/raw/endur/dl800-new_amended_cancelled_deals/dl800-new_amended_cancelled_deals"),
    error_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/errors/dl800_new_amended_cancelled_deals_errors"),
    log_output=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/log/dl800_new_amended_cancelled_deals_log"),
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
        logger,
        error_output,
        log_output,
        )
