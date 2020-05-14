# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL800_FLUSH, DL800_PROFILE


@configure(profile=DL800_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/deals/history/DEAL800_New_Amended_Cancelled_Deals"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl800_new_amended_cancelled_deals"),
)
def my_compute_function(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl800 transformed and appends it onto dl800 history

    Args:
        params1 (sparkContext): ctx
        params2 (IncrementalInput): transformed dl800
        params3 (IncrementalOutput): history dl800

    Returns:
        dataframe: history dl120
    """
    if DL800_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
