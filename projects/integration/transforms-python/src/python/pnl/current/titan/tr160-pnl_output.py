# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import TR160_FLUSH, TR160_FLUSH_START_DATE, TR160_PROFILE


@configure(profile=TR160_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/current/titan/tr160-pnl_output"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/typed/titan/tr160-pnl_output"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters tr160 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed tr160

    Returns:
        dataframe: current tr160
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=TR160_FLUSH,
                    flush_start_date=TR160_FLUSH_START_DATE)
    df = df.filter((df.COB_Date >= config.start_date) & (df.COB_Date <= config.end_date))
    df = filter_latest_runs(df, 'COB_Date', 'Run_Datetime')
    return df
