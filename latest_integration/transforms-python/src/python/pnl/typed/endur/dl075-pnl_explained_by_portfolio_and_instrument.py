# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, incremental, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_typed, reorder_columns
from python.pnl.schema.schema_dl075_pnl_explained_by_portfolio_and_instrument import typed_output_schema


def dl075_typed_transformed(df):

    df = compute_typed(df, typed_output_schema)
    df = reorder_columns(df, typed_output_schema)

    return df


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl075-pnl_explained_by_portfolio_and_instrument"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl075-pnl_explained_by_portfolio_and_instrument/dl075-pnl_explained_by_portfolio_and_instrument"),
)
def my_compute_function(df):
    '''
    Version: V0
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        params1 (dataframe): raw dl075

    Returns:
        dataframe: typed dl075
    '''
    df = dl075_typed_transformed(df)
    return df
