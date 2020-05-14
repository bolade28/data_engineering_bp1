# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL020_FLUSH, DL020_PROFILE


@configure(profile=DL020_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/dl020_pnl_detail_mtd"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl020_pnl_detail_mtd")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl020 transformed and appends it onto dl020 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): transformed dl020
        params3 (dataframe): history dl020

    Returns:
        dataframe: history dl020
    """
    if DL020_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
