# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL055_FLUSH, DL055_PROFILE


@configure(profile=DL055_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/dl055_pnl_adjustments"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl055_pnl_adjustments")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl055 transformed and appends it onto dl055 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): transformed dl055
        params3 (dataframe): history dl055

    Returns:
        dataframe: history dl055
    """
    if DL055_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
