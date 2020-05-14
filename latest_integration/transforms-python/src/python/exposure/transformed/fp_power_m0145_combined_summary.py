# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions


@transform_df(
        Output("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m0145_combined_summary"),
        df=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m0145_combined_detail")
)
def compute_function(df):
    """
    Transformation stage function
    Version: T2
    This function:
        For generating summary transformation.

    Args:
        df (dataframe): detailed fp_power_145m

    Returns:
        dataframe: transformed fp_power_145m
    """
    return df
