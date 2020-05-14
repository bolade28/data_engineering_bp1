# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
import python.util.bp_reorder as bp_reorder
from python.pnl.schema.schema_dl010_pnl_detail import transformed_output_schema
from python.bp_flush_control import DL010_PROFILE


@configure(profile=DL010_PROFILE)
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl010_pnl_detail"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl010_pnl_detail"),
)
def dtd_calc(df):
    """ Transformation stage function
    Version: T0
    This function:
        Day to day calcuations
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl010

    Returns:
        dataframe: transformed dl010

    """
    '''
    # Prevent calculations from overriding columns
    # TODO: if null -> use this calculation, else, dont use
    df = df.withColumn("DTD_Real_Disc_Pf", f.col("base_realized_value") - f.col("yest_base_realized_value")) \
           .withColumn("DTD_Unreal_Disc_Pf", f.col("base_unrealized_value") - f.col("yest_base_unrealized_value")) \
           .withColumn("DTD_Total_PnL_Disc_Pf", f.col("base_total_value") - f.col("yest_base_total_value")) \
           .withColumn("DTD_Real_Disc_Deal", f.col("realized_value") - f.col("yest_realized_value")) \
           .withColumn("DTD_Unreal_Disc_Deal", f.col("unrealized_value") - f.col("yest_unrealized_value")) \
           .withColumn("DTD_Total_PnL_Disc_Deal", f.col("total_value") - f.col("yest_total_value"))
    '''
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date") \
           .withColumnRenamed("deal_num", "Deal_Number") \
           .withColumnRenamed("tran_status", "Tran_Status") \
           .withColumnRenamed("start_date", "Start_Date") \
           .withColumnRenamed("end_date", "End_Date") \
           .withColumnRenamed("base_realized_value", "LTD_Real_Disc_Pf") \
           .withColumnRenamed("base_unrealized_value", "LTD_Unreal_Disc_Pf") \
           .withColumnRenamed("base_total_value", "LTD_Total_PnL_Disc_Pf") \
           .withColumnRenamed("realized_value", "LTD_Real_Disc_Deal") \
           .withColumnRenamed("unrealized_value", "LTD_Unreal_Disc_Deal") \
           .withColumnRenamed("total_value", "LTD_Total_PnL_Disc_Deal") \
           .withColumnRenamed("yest_base_realized_value", "Yest_LTD_Real_Disc_Pf") \
           .withColumnRenamed("yest_base_unrealized_value", "Yest_LTD_Unreal_Disc_Pf") \
           .withColumnRenamed("yest_base_total_value", "Yest_LTD_Total_PnL_Disc_Pf") \
           .withColumnRenamed("yest_realized_value", "Yest_LTD_Real_Disc_Deal") \
           .withColumnRenamed("yest_unrealized_value", "Yest_LTD_Unreal_Disc_Deal") \
           .withColumnRenamed("yest_total_value", "Yest_LTD_Total_PnL_Disc_Deal") \
           .withColumnRenamed("pymt", "Pymt") \
           .withColumnRenamed("yest_pymt", "Yest_Pymt") \
           .withColumnRenamed("yest_tran_status", "Yest_Tran_Status") \
           .withColumnRenamed("internal_portfolio", "Internal_Portfolio") \
           .withColumnRenamed("param_seq_num", "Param_Seq_Num") \
           .withColumnRenamed("param_seq_num_1", "Param_Seq_Num_1") \
           .withColumnRenamed("Scenario_ID", "Scenario_Id") \
           .withColumnRenamed("broker_fee_type", "Broker_Fee_Type") \
           .withColumnRenamed("cflow_type", "Cflow_Type") \
           .withColumnRenamed("comm_opt_exercised_flag", "Comm_Opt_Exerc_Flag") \
           .withColumnRenamed("currency_id", "Currency_Id") \
           .withColumnRenamed("df", "Df") \
           .withColumnRenamed("event_source_id", "Event_Source_Id") \
           .withColumnRenamed("ins_num", "Ins_Num") \
           .withColumnRenamed("ins_seq_num", "Ins_Seq_Num") \
           .withColumnRenamed("ins_source_id", "Ins_Source_Id") \
           .withColumnRenamed("new_deal", "New_Deal") \
           .withColumnRenamed("price", "Price") \
           .withColumnRenamed("price_band", "Price_Band") \
           .withColumnRenamed("price_band_seq_num", "Price_Band_Seq_Num") \
           .withColumnRenamed("profile_seq_num", "Profile_Seq_Num") \
           .withColumnRenamed("pymt_date", "Pymt_Date") \
           .withColumnRenamed("rate_dtmn_date", "Rate_Dtmn_Date") \
           .withColumnRenamed("rate_status", "Rate_Status") \
           .withColumnRenamed("settlement_type", "Settlement_Type") \
           .withColumnRenamed("strike", "Strike") \
           .withColumnRenamed("tran_num", "Tran_Num") \
           .withColumnRenamed("volume", "Volume")

    reordered_schema = bp_reorder.schema_reader(transformed_output_schema)
    df = df.select(*reordered_schema)

    return df
