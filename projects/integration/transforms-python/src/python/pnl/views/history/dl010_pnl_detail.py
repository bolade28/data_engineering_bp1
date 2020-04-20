# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL010_FLUSH, DL010_PROFILE


@configure(profile=DL010_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/dl010_pnl_detail"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl010_pnl_detail")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl010 transformed and appends it onto dl010 history

    Args:
        params1 (sparkContext): ctx
        params2 (IncrementalInput): transformed dl010
        params3 (IncrementalOutput): history dl010

    Returns:
        dataframe: history dl010
    """
    if DL010_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
