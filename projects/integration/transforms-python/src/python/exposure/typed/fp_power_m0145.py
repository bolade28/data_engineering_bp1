from transforms.api import transform_df, incremental, Input, Output

from python.util.transforms_util import compute_typed, reorder_columns
from python.exposure.schema_exposure.schema_fp_power_m0145_latest import typed_output_schema


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m0145"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_power_m0145/fp_power_m0145")
)
def my_compute_function(df):
    """
    Transformation stage function
    Version: T2
    This function:
        For processing data fram raw, rename and re-ordering.
        Reorders the dataframe based on its respective output schema

    Args:
        df (dataframe): current fp_power_145m

    Returns:
        dataframe: transformed fp_power_145m
    """
    df = compute_typed(df, typed_output_schema)
    df = reorder_columns(df, typed_output_schema)

    return df
