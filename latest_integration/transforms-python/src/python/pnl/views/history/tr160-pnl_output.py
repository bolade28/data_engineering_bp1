# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import TR160_FLUSH, TR160_PROFILE


@configure(profile=TR160_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/lng/pnl/history/PNL160_Physical_LNG"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/tr160-pnl_output")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from tr160 transformed and appends it onto tr160 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): transformed tr160
        params3 (dataframe): latest tr160

    Returns:
        dataframe: history tr160
    """
    if TR160_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
