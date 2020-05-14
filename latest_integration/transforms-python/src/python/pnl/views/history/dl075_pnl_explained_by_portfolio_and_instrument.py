# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL075_FLUSH, DL075_PROFILE


@configure(profile=DL075_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/PNL075_Explained_By_Portfolio_And_Instrument"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl075_pnl_explained_by_portfolio_and_instrument")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl075 transformed and appends it onto dl075 history

    Args:
        params1 (sparkContext): ctx
        params2 (IncrementalInput): transformed dl075
        params3 (IncrementalOutput): history dl075
    
    Returns:
        dataframe: history dl075

    """
    if DL075_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
