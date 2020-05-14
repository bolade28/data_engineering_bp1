# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import TR200_FLUSH, TR200_FLUSH_START_DATE, TR200_PROFILE


def convert(df):
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=TR200_FLUSH,
                    flush_start_date=TR200_FLUSH_START_DATE)
    df = df.filter((df.Current_COB_Date >= config.start_date) & (df.Current_COB_Date <= config.end_date))
    df = filter_latest_runs(df, 'Current_COB_Date', 'RunDateTime')
    return df


@configure(profile=TR200_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/current/titan/tr200_exposure"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/typed/titan/tr200_exposure"),
)
def my_compute_function(df):
    return convert(df)
