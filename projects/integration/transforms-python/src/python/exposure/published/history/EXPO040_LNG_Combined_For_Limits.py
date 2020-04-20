# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import EXPO040_FLUSH, EXPO040_PROFILE
from python.util.bp_constants import SOURCE_COL


@configure(profile=EXPO040_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/lng/exposure/history/EXPO040_LNG_Combined_For_Limits"),
    input_data=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/LNG_combined_for_limits")
)
def my_compute_function(ctx, input_data, output_data):
    '''
    Version: H1
    '''
    if EXPO040_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data, source_col=SOURCE_COL)

    output_data.write_dataframe(output_df)
