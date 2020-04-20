# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL120_FLUSH, DL120_FLUSH_START_DATE


@configure(profile=['NUM_EXECUTORS_8', 'EXECUTOR_MEMORY_EXTRA_SMALL'])
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/current/endur/dl120-market_risk_stage_result"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/typed/endur/dl120-market_risk_stage_result"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters dl120 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed dl120

    Returns:
        dataframe: current dl120
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL120_FLUSH,
                    flush_start_date=DL120_FLUSH_START_DATE)

    df = df.filter((df.Analysis_Date >= config.start_date) & (df.Analysis_Date <= config.end_date))
    df = filter_latest_runs(df, 'Analysis_Date', 'Run_Datetime')
    return df
