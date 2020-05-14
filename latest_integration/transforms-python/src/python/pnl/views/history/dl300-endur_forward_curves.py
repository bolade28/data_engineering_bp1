# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL300_FLUSH, DL300_PROFILE


@configure(profile=DL300_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/prices/history/PRICE300_Endur_forward_curves"),
    input_data=Input("/BP/IST-IG-DD/data/technical/prices/transformed/dl300_endur_forward_curves")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl300 Transformed and appends it onto dl300 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): Transformed dl300
        params3 (dataframe): history dl300

    Returns:
        dataframe: history dl300
    """
    if DL300_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(ctx, input_data, output_data)

    output_data.write_dataframe(output_df)
