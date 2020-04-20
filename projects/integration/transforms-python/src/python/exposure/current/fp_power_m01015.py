# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import FPPOWER1015_FLUSH, FPPOWER1015_FLUSH_START_DATE, FPPOWER1015_PROFILE


def convert(df):
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=FPPOWER1015_FLUSH,
                    flush_start_date=FPPOWER1015_FLUSH_START_DATE)
    df = df.filter((df.PLEX_Calculation_Date >= config.start_date) & (df.PLEX_Calculation_Date <= config.end_date))
    df = filter_latest_runs(df, 'PLEX_Calculation_Date', 'Run_Datetime')
    return df


@configure(profile=FPPOWER1015_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/current/freeport/fp_power_m01015"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m01015"),
)
def my_compute_function(df):
    return convert(df)
