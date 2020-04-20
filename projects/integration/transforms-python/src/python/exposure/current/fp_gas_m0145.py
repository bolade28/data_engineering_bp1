# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import FPGAS145_FLUSH, FPGAS145_FLUSH_START_DATE, FPGAS145_PROFILE


def convert(df):
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=FPGAS145_FLUSH,
                    flush_start_date=FPGAS145_FLUSH_START_DATE)
    df = df.filter((df.Analysis_Date >= config.start_date) & (df.Analysis_Date <= config.end_date))
    df = filter_latest_runs(df, 'Analysis_Date', 'Run_Datetime')
    return df


@configure(profile=FPGAS145_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/current/freeport/fp_gas_m0145"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_gas_m0145"),
)
def my_compute_function(df):
    """
    Transformation stage function
    Version: T2
    This function:
        For processing current data.
        Reorders the dataframe based on its respective output schema

    Args:
        df (dataframe): current fp_gas_145m

    Returns:
        dataframe: transformed fp_gas_145m
    """
    return convert(df)
