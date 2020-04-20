# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.util.bp_historical_file_utils import filter_latest_data, add_hist_col
from python.bp_flush_control import EXPO130_FLUSH, EXPO130_PROFILE


@configure(profile=EXPO130_PROFILE)
@incremental(snapshot_inputs=['input_data'])
@transform(
    output_data=Output("/BP/IST-IG-DD/data/published/all/exposure/history/EXPO130_Endur_Monthly_Summary"),
    input_data=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/dl130_market_risk_stage_result_summary_conformed_optimised"),
)
def my_compute_function(input_data, output_data):
    '''
    Version: H1
    '''
    if EXPO130_FLUSH:
        output_data.set_mode("replace")
        output_df = add_hist_col(input_data.dataframe())
    else:
        output_df = filter_latest_data(input_data, output_data)

    output_df = output_df.withColumnRenamed('Portfolio/Book', "Portfolio")
    output_data.write_dataframe(output_df)
