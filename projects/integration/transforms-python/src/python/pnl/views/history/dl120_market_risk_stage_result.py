# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL120_FLUSH, DL120_PROFILE


@configure(profile=DL120_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/technical/exposure/history/history/dl120_market_risk_stage_result"),
    input_data=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/dl120_market_risk_stage_result")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl120 transformed and appends it onto dl120 history

    Args:
        params1 (sparkContext): ctx
        params2 (IncrementalInput): transformed dl120
        params3 (IncrementalOutput): history dl120
    
    Returns:
        dataframe: history dl120
    """
    if DL120_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
