# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, incremental

# ===== import: our functions


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl035-pnl_by_portfolio_and_instrument"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl035-pnl_by_portfolio_and_instrument/dl035-pnl_by_portfolio_and_instrument"),
)
def my_compute_function(df):
    '''
    Version: V0
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        params1 (dataframe): raw dl035

    Returns:
        dataframe: typed dl035
    '''
    df = df.select(
        f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
        f.to_date(df['Reval_Date'], 'yyyy-MM-dd').alias('Reval_Date'),
        df['Internal_Portfolio'],
        df['Portfolio_Currency'],
        df['Instrument_Type'],
        df['dtd_pnl_disc_total_pflo_ccy'].cast('double'),
        df['dtd_pnl_disc_real_pflo_ccy'].cast('double'),
        df['dtd_pnl_disc_unreal_pflo_ccy'].cast('double'),
        df['dtd_pnl_undisc_total_pflo_ccy'].cast('double'),
        df['dtd_pnl_undisc_real_pflo_ccy'].cast('double'),
        df['dtd_pnl_undisc_unreal_pflo_ccy'].cast('double'),
        df['mtd_pnl_disc_total_pflo_ccy'].cast('double'),
        df['mtd_pnl_disc_real_pflo_ccy'].cast('double'),
        df['mtd_pnl_disc_unreal_pflo_ccy'].cast('double'),
        df['mtd_pnl_undisc_total_pflo_ccy'].cast('double'),
        df['mtd_pnl_undisc_real_pflo_ccy'].cast('double'),
        df['mtd_pnl_undisc_unreal_pflo_ccy'].cast('double'),
        df['ytd_pnl_disc_total_pflo_ccy'].cast('double'),
        df['ytd_pnl_disc_real_pflo_ccy'].cast('double'),
        df['ytd_pnl_disc_unreal_pflo_ccy'].cast('double'),
        df['ytd_pnl_undisc_total_pflo_ccy'].cast('double'),
        df['ytd_pnl_undisc_real_pflo_ccy'].cast('double'),
        df['ytd_pnl_undisc_unreal_pflo_ccy'].cast('double'),
        df['ltd_pnl_disc_real_pflo_ccy'].cast('double'),
        df['ltd_pnl_disc_total_pflo_ccy'].cast('double'),
        df['ltd_pnl_disc_unreal_pflo_ccy'].cast('double'),
        df['ltd_pnl_undisc_real_pflo_ccy'].cast('double'),
        df['ltd_pnl_undisc_total_pflo_ccy'].cast('double'),
        df['ltd_pnl_undisc_unreal_pflo_ccy'].cast('double'),
        df['Sub_Group'],
    )
    return df
