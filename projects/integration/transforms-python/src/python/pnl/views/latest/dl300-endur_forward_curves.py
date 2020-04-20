# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_LATEST
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/prices/latest/PRICE300_Endur_forward_curves"),
    df=Input("/BP/IST-IG-DD/data/published/all/prices/history/PRICE300_Endur_forward_curves"),
)
def my_compute_function(ctx, df):
    """
    Version : H1
    This function:
        Gets the latest run from each valuation from dl310 history and appends it onto dl300 latest

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): history dl300
        params3 (dataframe): latest dl300

    Returns:
        dataframe: latest dl300
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_LATEST, VALUATION_DATE_COL)
