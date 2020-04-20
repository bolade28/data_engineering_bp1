# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
import python.util.bp_reorder as bp_reorder
from python.pnl.schema.schema_dl020_pnl_detail_mtd import transformed_output_schema
from python.bp_flush_control import DL020_PROFILE


@configure(profile=DL020_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl020_pnl_detail_mtd"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl020-pnl_detail_mtd"),
)
def dtd_calc(df):
    """ Transformation stage function
    Version: T0
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl020

    Returns:
        dataframe: transformed dl020
    """
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date") \
        .withColumnRenamed("Deal_Num", "Deal_Number") \
        .withColumnRenamed("tran_status", "Tran_Status") \
        .withColumnRenamed("start_date", "Start_Date") \
        .withColumnRenamed("end_date", "End_Date") \
        .withColumnRenamed("base_realized_value", "MTD_Real_Disc_Pf") \
        .withColumnRenamed("base_unrealized_value", "MTD_Unreal_Disc_Pf") \
        .withColumnRenamed("base_total_value", "MTD_Total_PnL_Disc_Pf") \
        .withColumnRenamed("realized_value", "MTD_Real_Disc_Deal") \
        .withColumnRenamed("unrealized_value", "MTD_Unreal_Disc_Deal") \
        .withColumnRenamed("total_value", "MTD_Total_PnL_Disc_Deal") \
        .withColumnRenamed("pymt", "Pymt") \
        .withColumnRenamed("Scenario_ID", "Scenario_Id") \
        .withColumnRenamed("broker_fee_type", "Broker_Fee_Type") \
        .withColumnRenamed("cflow_type", "Cflow_Type") \
        .withColumnRenamed("comm_opt_exercised_flag", "Comm_Opt_Exercised_Flag") \
        .withColumnRenamed("currency_id", "Currency_Id") \
        .withColumnRenamed("df", "Df") \
        .withColumnRenamed("event_source_id", "Event_Source_Id") \
        .withColumnRenamed("ins_num", "Ins_Num") \
        .withColumnRenamed("ins_seq_num", "Ins_Seq_Num") \
        .withColumnRenamed("ins_source_id", "Ins_Source_Id") \
        .withColumnRenamed("price", "Price") \
        .withColumnRenamed("price_band", "Price_Band") \
        .withColumnRenamed("price_band_seq_num", "Price_Band_Seq_Num") \
        .withColumnRenamed("pymt_date", "Pymt_Date") \
        .withColumnRenamed("rate_dtmn_date", "Rate_Dtmn_Date") \
        .withColumnRenamed("rate_status", "Rate_Status") \
        .withColumnRenamed("settlement_type", "Settlement_Type") \
        .withColumnRenamed("strike", "Strike") \
        .withColumnRenamed("tran_num", "Tran_Num") \
        .withColumnRenamed("volume", "Volume") \
        .withColumnRenamed("deal_leg", "Deal_Leg") \
        .withColumnRenamed("deal_leg_1", "Deal_Leg_1") \
        .withColumnRenamed("deal_pdc", "Deal_Pdc")

    reordered_schema = bp_reorder.schema_reader(transformed_output_schema)
    df = df.select(*reordered_schema)

    return df
