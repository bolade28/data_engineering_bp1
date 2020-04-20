# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_RECENT
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/pnl/recent/dl030_pnl_detail_ytd"),
    df=Input("/BP/IST-IG-DD/data/published/all/pnl/history/dl030_pnl_detail_ytd"),
)
def my_compute_function(ctx, df):
    """
    Version : H1
    This function:
        Gets the recent run from each valuation from dl030 history and appends it onto dl030 recent

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): history dl030
        params3 (dataframe): recent dl030

    Returns:
        dataframe: recent dl030
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_RECENT, VALUATION_DATE_COL)
