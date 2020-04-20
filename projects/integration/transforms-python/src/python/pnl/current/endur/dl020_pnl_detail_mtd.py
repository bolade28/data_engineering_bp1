# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL020_FLUSH, DL020_FLUSH_START_DATE, DL020_PROFILE


@configure(profile=DL020_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl020-pnl_detail_mtd"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl020-pnl_detail_mtd"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters dl020 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed dl020

    Returns:
        dataframe: current dl020
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL020_FLUSH,
                    flush_start_date=DL020_FLUSH_START_DATE)

    df = df.filter((df.Reval_Date >= config.start_date) & (df.Reval_Date <= config.end_date))
    df = filter_latest_runs(df, 'Reval_Date', 'Run_Datetime')
    return df
