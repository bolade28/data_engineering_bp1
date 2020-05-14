# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_LATEST
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/pnl/latest/PNL080_By_Deal"),
    df=Input("/BP/IST-IG-DD/data/published/all/pnl/history/PNL080_By_Deal"),
)
def my_compute_function(df):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl080 history and appends it onto dl080 latest

    Args:
        df: history dl080

    Returns:
        dataframe: latest dl080
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_LATEST, VALUATION_DATE_COL)
