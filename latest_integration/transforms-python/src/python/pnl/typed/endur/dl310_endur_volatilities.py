# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output
# ===== import: our functions
from python.pnl.schema.schema_dl310_endur_volatilities import typed_output_schema
from python.util.schema_utils import compute_typed, reorder_columns


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/prices/typed/endur/dl310_endur_volatilities"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl310-endur_volatilities/dl310-endur_volatilities"),
)
def type_compute_function(df):
    '''
    Version: V0
    '''
    df = compute_typed(df, typed_output_schema)
    # Reorder according to business requirements
    df = reorder_columns(df, typed_output_schema)
    return df
