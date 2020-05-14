# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL125_FLUSH, DL125_FLUSH_START_DATE, DL125_PROFILE


@configure(profile=DL125_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl125-market_risk_stage_result_emission"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl125-market_risk_stage_result_emission"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters dl125 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed dl125

    Returns:
        dataframe: current dl125
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL125_FLUSH,
                    flush_start_date=DL125_FLUSH_START_DATE)

    df = df.filter((df.Analysis_Date >= config.start_date) & (df.Analysis_Date <= config.end_date))
    df = filter_latest_runs(df, 'Analysis_Date', 'Run_Datetime')
    return df
