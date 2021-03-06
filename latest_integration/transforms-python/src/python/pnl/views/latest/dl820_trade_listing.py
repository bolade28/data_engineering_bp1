# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_LATEST
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/deals/latest/DEAL820_Trade_Listing"),
    df=Input("/BP/IST-IG-DD/data/published/all/deals/history/DEAL820_Trade_Listing"),
)
def my_compute_function(ctx, df):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl820 history and appends it onto dl820 latest

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): history dl820
        params3 (dataframe): latest dl820

    Returns:
        dataframe: latest dl820
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_LATEST, VALUATION_DATE_COL)
