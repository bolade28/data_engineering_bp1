# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import FPPOWER145_FLUSH, FPPOWER145_FLUSH_START_DATE, FPPOWER145_PROFILE


def convert(df):
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=FPPOWER145_FLUSH,
                    flush_start_date=FPPOWER145_FLUSH_START_DATE)
    df = df.filter((df.Analysis_Date >= config.start_date) & (df.Analysis_Date <= config.end_date))
    df = filter_latest_runs(df, 'Analysis_Date', 'Run_Datetime')
    return df


@configure(profile=FPPOWER145_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/current/freeport/fp_power_m0145"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m0145"),
)
def my_compute_function(df):
    """
    Transformation stage function
    Version: T2
    This function:
        For processing current data from typed

    Args:
        df (dataframe): current fp_power_145m

    Returns:
        dataframe: transformed fp_power_145m
    """
    return convert(df)
