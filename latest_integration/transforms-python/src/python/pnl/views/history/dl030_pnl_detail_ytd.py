# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL030_FLUSH, DL030_PROFILE


@configure(profile=DL030_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/dl030_pnl_detail_ytd"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl030_pnl_detail_ytd")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl030 transformed and appends it onto dl030 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): transformed dl030
        params3 (dataframe): history dl030

    Returns:
        dataframe: history dl030
    """
    if DL030_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
