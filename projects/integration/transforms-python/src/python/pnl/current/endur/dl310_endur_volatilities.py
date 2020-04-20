# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL310_FLUSH, DL310_FLUSH_START_DATE


FLUSH = DL310_FLUSH
FLUSH_START_DATE = DL310_FLUSH_START_DATE


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/prices/current/endur/dl310_endur_volatilities"),
    df=Input("/BP/IST-IG-DD/data/technical/prices/typed/endur/dl310_endur_volatilities"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters dl310 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed dl310

    Returns:
        dataframe: current dl310
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL310_FLUSH,
                    flush_start_date=DL310_FLUSH_START_DATE)
    df = df.filter((df.Valuation_Date >= config.start_date) & (df.Valuation_Date <= config.end_date))
    df = filter_latest_runs(df, 'Valuation_Date', 'Run_Datetime')
    return df
