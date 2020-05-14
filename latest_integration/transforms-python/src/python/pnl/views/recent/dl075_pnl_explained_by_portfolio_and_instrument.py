# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_RECENT
from python.util.bp_constants import VALUATION_DATE_COL
from python.util.bp_historical_file_utils import restrict_to_top_dates


@transform_df(
    Output("/BP/IST-IG-DD/data/published/all/pnl/recent/PNL075_Explained_By_Portfolio_And_Instrument"),
    df=Input("/BP/IST-IG-DD/data/published/all/pnl/history/PNL075_Explained_By_Portfolio_And_Instrument"),
)
def my_compute_function(ctx, df):
    """
    Version : H1
    This function:
        Gets the recent run from each valuation from dl075 history and appends it onto dl075 recent

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): history dl075
        params3 (dataframe): recent dl075

    Returns:
        dataframe: recent dl075
    """
    return restrict_to_top_dates(df, LOOK_BACK_PERIOD_RECENT, VALUATION_DATE_COL)
