# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
import python.util.bp_reorder as bp_reorder
from python.pnl.schema.schema_dl125_market_risk_stage_result_emission import transformed_output_schema
from python.bp_flush_control import DL125_PROFILE


@configure(profile=DL125_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl125_market_risk_stage_result_emission"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl125-market_risk_stage_result_emission"),
)
def dtd_calc(df):
    """ Transformation stage function
    Version: T0
    This function:
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl125

    Returns:
        dataframe: transformed dl125
    """
    df = df.withColumnRenamed("Analysis_Date", "Valuation_Date")
    reordered_schema = bp_reorder.schema_reader(transformed_output_schema)
    df = df.select(*reordered_schema)

    return df
