# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL035_FLUSH, DL035_PROFILE


@configure(profile=DL035_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/dl035_pnl_by_portfolio_and_instrument"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl035_pnl_by_portfolio_and_instrument")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl035 transformed and appends it onto dl035 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): transformed dl035
        params3 (dataframe): history dl035

    Returns:
        dataframe: history dl035
    """
    if DL035_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
