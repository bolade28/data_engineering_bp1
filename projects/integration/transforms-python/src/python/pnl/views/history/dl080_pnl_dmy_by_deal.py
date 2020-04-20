# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL080_FLUSH, DL080_PROFILE


@configure(profile=DL080_PROFILE)
@incremental(snapshot_inputs=['transform_input'])
@transform(
    transform_output=Output("/BP/IST-IG-DD/data/published/all/pnl/history/PNL080_By_Deal"),
    transform_input=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl080_pnl_dmy_by_deal")
)
def get_latest_valuations(transform_input, transform_output):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl080 transformed and appends it onto dl080 history

    Args:
        transform_input (IncrementalInput): transformed dl080
        transform_output (IncrementalOutput): history dl080

    Returns:
        dataframe: history dl080

    """
    if DL080_FLUSH:
        transform_output.set_mode("replace")
        output_df = add_hist_col(transform_input.dataframe())
    else:
        output_df = filter_latest_data(transform_input, transform_output)

    transform_output.write_dataframe(output_df)
