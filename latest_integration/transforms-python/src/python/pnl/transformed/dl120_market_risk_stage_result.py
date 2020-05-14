# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
import python.util.bp_reorder as bp_reorder
from python.pnl.schema.schema_dl120_market_risk_stage_result import transformed_output_schema


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/transformed/dl120_market_risk_stage_result"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/current/endur/dl120-market_risk_stage_result"),
)
def dtd_calc(df):
    """ Transformation stage function
    Version: T0
    This function:
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl120

    Returns:
        dataframe: transformed dl120
    """
    df = df.withColumnRenamed("Analysis_Date", "Valuation_Date")
    reordered_schema = bp_reorder.schema_reader(transformed_output_schema)
    df = df.select(*reordered_schema)

    return df
