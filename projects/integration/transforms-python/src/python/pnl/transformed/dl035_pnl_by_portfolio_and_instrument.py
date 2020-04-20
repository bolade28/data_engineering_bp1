# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
import python.util.bp_reorder as bp_reorder
from python.pnl.schema.schema_dl035_pnl_by_portfolio_and_instrument import transformed_output_schema


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl035_pnl_by_portfolio_and_instrument"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl035-pnl_by_portfolio_and_instrument"),
)
def dtd_calc(df):
    """ Transformation stage function
    Version: T0
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl035

    Returns:
        dataframe: transformed dl035
    """
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date") \
        .withColumnRenamed("dtd_pnl_disc_total_pflo_ccy", "DTD_Disc_Total_Pf") \
        .withColumnRenamed("dtd_pnl_disc_real_pflo_ccy", "DTD_Disc_Real_Pf") \
        .withColumnRenamed("dtd_pnl_disc_unreal_pflo_ccy", "DTD_Disc_Unreal_Pf") \
        .withColumnRenamed("dtd_pnl_undisc_total_pflo_ccy", "DTD_Undisc_Total_Pf") \
        .withColumnRenamed("dtd_pnl_undisc_real_pflo_ccy", "DTD_Undisc_Real_Pf") \
        .withColumnRenamed("dtd_pnl_undisc_unreal_pflo_ccy", "DTD_Undisc_Unreal_Pf") \
        .withColumnRenamed("mtd_pnl_disc_total_pflo_ccy", "MTD_Disc_Total_Pf") \
        .withColumnRenamed("mtd_pnl_disc_real_pflo_ccy", "MTD_Disc_Real_Pf") \
        .withColumnRenamed("mtd_pnl_disc_unreal_pflo_ccy", "MTD_Disc_Unreal_Pf") \
        .withColumnRenamed("mtd_pnl_undisc_total_pflo_ccy", "MTD_Undisc_Total_Pf") \
        .withColumnRenamed("mtd_pnl_undisc_real_pflo_ccy", "MTD_Undisc_Real_Pf") \
        .withColumnRenamed("mtd_pnl_undisc_unreal_pflo_ccy", "MTD_Undisc_Unreal_Pf") \
        .withColumnRenamed("ytd_pnl_disc_total_pflo_ccy", "YTD_Disc_Total_Pf") \
        .withColumnRenamed("ytd_pnl_disc_real_pflo_ccy", "YTD_Disc_Real_Pf") \
        .withColumnRenamed("ytd_pnl_disc_unreal_pflo_ccy", "YTD_Disc_Unreal_Pf") \
        .withColumnRenamed("ytd_pnl_undisc_total_pflo_ccy", "YTD_Undisc_Total_Pf") \
        .withColumnRenamed("ytd_pnl_undisc_real_pflo_ccy", "YTD_Undisc_Real_Pf") \
        .withColumnRenamed("ytd_pnl_undisc_unreal_pflo_ccy", "YTD_Undisc_Unreal_Pf") \
        .withColumnRenamed("ltd_pnl_disc_total_pflo_ccy", "LTD_Disc_Total_Pf") \
        .withColumnRenamed("ltd_pnl_disc_real_pflo_ccy", "LTD_Disc_Real_Pf") \
        .withColumnRenamed("ltd_pnl_disc_unreal_pflo_ccy", "LTD_Disc_Unreal_Pf") \
        .withColumnRenamed("ltd_pnl_undisc_total_pflo_ccy", "LTD_Undisc_Total_Pf") \
        .withColumnRenamed("ltd_pnl_undisc_real_pflo_ccy", "LTD_Undisc_Real_Pf") \
        .withColumnRenamed("ltd_pnl_undisc_unreal_pflo_ccy", "LTD_Undisc_Unreal_Pf") \

    reordered_schema = bp_reorder.schema_reader(transformed_output_schema)
    df = df.select(*reordered_schema)

    return df
