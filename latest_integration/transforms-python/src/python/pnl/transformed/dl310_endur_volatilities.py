# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import reorder_columns
from python.pnl.schema.schema_dl310_endur_volatilities import transformed_output_schema


def dl310_transformed(df):

    # Reorder according to business requirements
    df = reorder_columns(df, transformed_output_schema)
    return df


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/prices/transformed/dl310_endur_volatilities"),
    df=Input("/BP/IST-IG-DD/data/technical/prices/current/endur/dl310_endur_volatilities"),
)
def dtd_calc(df):
    """ Transformation stage function
    This function:
        Version: T1
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl310

    Returns:
        dataframe: transformed dl310
    """
    df = dl310_transformed(df)

    return df
