# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL820_FLUSH, DL820_PROFILE


@configure(profile=DL820_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/deals/history/DEAL820_Trade_Listing"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl820_trade_listing")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl820 Transformed and appends it onto dl820 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): Transformed dl820
        params3 (dataframe): history dl820

    Returns:
        dataframe: history dl820
    """
    if DL820_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
