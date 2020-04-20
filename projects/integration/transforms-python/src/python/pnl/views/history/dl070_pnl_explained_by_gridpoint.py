# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL070_FLUSH, DL070_PROFILE


@configure(profile=DL070_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/pnl/history/PNL070_Explained_By_Gridpoint"),
    input_data=Input("/BP/IST-IG-DD/data/technical/pnl/transformed/dl070_pnl_explained_by_gridpoint")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl070 transformed and appends it onto dl070 history

    Args:
        params1 (sparkContext): ctx
        params2 (IncrementalInput): transformed dl070
        params3 (IncrementalOutput): history dl070
    
    Returns:
        dataframe: history dl070
    """
    if DL070_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
