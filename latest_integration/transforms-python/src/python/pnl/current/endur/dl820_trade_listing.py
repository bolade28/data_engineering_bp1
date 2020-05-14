# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.bp_dateutils import filter_latest_runs
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG
from python.bp_flush_control import DL820_FLUSH, DL820_FLUSH_START_DATE
from pyspark.sql.types import DateType

# TODO: consider a more elegant way of doing this
FLUSH = DL820_FLUSH
FLUSH_START_DATE = DL820_FLUSH_START_DATE


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl820_trade_listing"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl820-trade_listing"),
)
def my_compute_function(ctx, df):
    """
    This function:
        filters dl820 for within the lookup period
        filters the latest runs for each valuation date based on the filtered data

    Args:
        params1 (sparkContext): ctx
        params2 (dataframe): typed dl820

    Returns:
        dataframe: current dl820
    """
    config = Config(look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=DL820_FLUSH,
                    flush_start_date=DL820_FLUSH_START_DATE)
    df = df.withColumn('Reval_Date_tmp',
                       f.when(
                        (df['Reval_Date'].isNull()),
                        df['Run_Datetime'].cast(DateType())
                        ).otherwise(
                        df['Reval_Date'])
                       ).drop(
            'Reval_Date'
        ).withColumnRenamed(
            'Reval_Date_tmp', 'Reval_Date'
        )

    df = df.filter((df.Reval_Date >= config.start_date) & (df.Reval_Date <= config.end_date))
    df = filter_latest_runs(df, 'Reval_Date', 'Run_Datetime')
    return df
