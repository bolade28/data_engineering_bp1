# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import TR200_FLUSH, TR200_PROFILE


@configure(profile=TR200_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/technical/exposure/history/tr200_exposure"),
    input_data=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/tr200_exposure_combined_summary")
)
def get_latest_valuations(input_data, output_data):
    """
    Version: H1
    """
    if TR200_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_data.write_dataframe(output_df)