# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, incremental, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_typed, reorder_columns
from python.pnl.schema.schema_dl070_pnl_explained_by_gridpoint import typed_output_schema


def dl070_typed_transform(df):

    df = compute_typed(df, typed_output_schema)
    df = reorder_columns(df, typed_output_schema)

    return df


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl070-pnl_explained_by_gridpoint"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl070-pnl_explained_by_gridpoint/dl070-pnl_explained_by_gridpoint"),
)
def my_compute_function(df):
    '''
    Version: V0
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        params1 (dataframe): raw dl070

    Returns:
        dataframe: typed dl070
    '''
    df = dl070_typed_transform(df)
    return df
