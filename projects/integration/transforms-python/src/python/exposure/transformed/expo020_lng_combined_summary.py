# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.exposure.schema_exposure.schema_expo020_lng_combined_all import combined_output_schema
from python.util.schema_utils import reorder_columns


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/transformed/EXPO020_LNG_Combined_Summary"),
    df_dl130_input=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/dl130_Endur_Monthly_Exposure_combined_Format"),
    df_fp_gas_m0145_input=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_gas_m0145_combined_summary"),
    df_fp_power_m0145_input=Input('/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m0145_combined_summary'),
    df_expo_titan_input=Input('/BP/IST-IG-DD/data/technical/exposure/transformed/tr200_exposure_combined_summary')
)
def myComputeFunction(
        df_dl130_input,	df_fp_gas_m0145_input,	df_fp_power_m0145_input, df_expo_titan_input
        ):

    # Reorder according to business requirements
    df_dl130_input = reorder_columns(df_dl130_input, combined_output_schema)
    df_fp_gas_m0145_input = reorder_columns(df_fp_gas_m0145_input, combined_output_schema)
    df_fp_power_m0145_input = reorder_columns(df_fp_power_m0145_input, combined_output_schema)
    df_expo_titan_input = reorder_columns(df_expo_titan_input, combined_output_schema)

    df_master_summary_combined = df_dl130_input.union(df_fp_gas_m0145_input).\
        union(df_fp_power_m0145_input).\
        union(df_expo_titan_input)

    return df_master_summary_combined
