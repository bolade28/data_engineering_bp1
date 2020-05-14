# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import EXPO020_FLUSH, EXPO020_PROFILE
from python.util.bp_constants import SOURCE_COL


@configure(profile=EXPO020_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/lng/exposure/history/EXPO020_LNG_Combined_Summary"),
    input_data=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/EXPO020_LNG_Combined_Summary")
)
def get_latest_valuations(ctx, input_data, output_data):
    """
    Version: H1 (by source)
    This function uses filter_latest_data_and_write_back (by source)
    """
    if EXPO020_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data, source_col=SOURCE_COL)

    output_data.write_dataframe(output_df)
