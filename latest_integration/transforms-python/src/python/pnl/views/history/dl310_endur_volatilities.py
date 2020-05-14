# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import DL310_FLUSH, DL310_PROFILE


@configure(profile=DL310_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/prices/history/PRICE310_Endur_Volatilities"),
    input_data=Input("/BP/IST-IG-DD/data/technical/prices/transformed/dl310_endur_volatilities")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl310 Transformed and appends it onto dl310 history

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): Transformed dl310
        params3 (dataframe): history dl310

    Returns:
        dataframe: history dl310
    """
    if DL310_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)
