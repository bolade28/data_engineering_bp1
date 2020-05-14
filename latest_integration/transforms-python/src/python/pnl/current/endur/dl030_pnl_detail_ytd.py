# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL030_FLUSH, DL030_FLUSH_START_DATE, DL030_PROFILE


@configure(profile=DL030_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl030-pnl_detail_ytd"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl030-pnl_detail_ytd"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters dl030 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed dl030

    Returns:
        dataframe: current dl030
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL030_FLUSH,
                    flush_start_date=DL030_FLUSH_START_DATE)

    df = df.filter((df.Reval_Date >= config.start_date) & (df.Reval_Date <= config.end_date))
    df = filter_latest_runs(df, 'Reval_Date', 'Run_Datetime')
    return df
