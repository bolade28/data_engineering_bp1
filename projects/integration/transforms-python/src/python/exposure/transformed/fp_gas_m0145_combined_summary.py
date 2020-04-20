# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.exposure.schema_exposure.schema_fp_gas_m0145_combined_detail import combined_output_schema
from python.util.schema_utils import compute_transform, reorder_columns


def convert(df):
    df_res = compute_transform(df, combined_output_schema)

    # Reorder according to business requirements
    df_res = reorder_columns(df, combined_output_schema)
    return df_res


@transform_df(
        Output("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_gas_m0145_combined_summary"),
        df=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_gas_m0145_combined_detail"),
)
def myComputeFunction(df):
    return convert(df)
