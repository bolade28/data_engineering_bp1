# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_RECENT
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/pnl/recent/dl035_pnl_by_portfolio_and_instrument"),
    df=Input("/BP/IST-IG-DD/data/published/all/pnl/history/dl035_pnl_by_portfolio_and_instrument"),
)
def my_compute_function(ctx, df):
    """
    Version : H1
    This function:
        Gets the recent run from each valuation from dl035 history and appends it onto dl035 recent

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): history dl035
        params3 (dataframe): recent dl035

    Returns:
        dataframe: recent dl035
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_RECENT, VALUATION_DATE_COL)
