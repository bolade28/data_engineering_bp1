# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_RECENT
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/pnl/recent/dl125_market_risk_stage_result_emission"),
    df=Input("/BP/IST-IG-DD/data/published/all/pnl/history/dl125_market_risk_stage_result_emission"),
)
def my_compute_function(ctx, df):
    """
    Version : H1
    This function:
        Gets the recent run from each valuation from dl125 history and appends it onto dl125 recent

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): history dl125
        params3 (dataframe): recent dl125

    Returns:
        dataframe: recent dl125
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_RECENT, VALUATION_DATE_COL)
