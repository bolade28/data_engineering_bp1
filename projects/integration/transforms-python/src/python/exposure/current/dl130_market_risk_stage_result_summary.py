# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL130_FLUSH, DL130_FLUSH_START_DATE, DL130_PROFILE


def convert(df):
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL130_FLUSH,
                    flush_start_date=DL130_FLUSH_START_DATE)
    df = df.filter((df.Analysis_Date >= config.start_date) & (df.Analysis_Date <= config.end_date))
    df = filter_latest_runs(df, 'Analysis_Date', 'Run_Datetime')
    return df


@configure(profile=DL130_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/current/endur/dl130_market_risk_stage_result_summary"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/typed/endur/dl130_market_risk_stage_result_summary"),
)
def my_compute_function(df):
    return convert(df)
