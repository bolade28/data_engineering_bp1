from transforms.api import transform, incremental, Input, Output
from pyspark.sql import functions as F


@incremental()
@transform(
    output_dataset=Output("/BP/IST-IG-DD/data/technical/exposure/typed/ingestion_tracker"),
    tr_dataset=Input("/BP/IST-IG-DD/data/technical/exposure/typed/titan/tr200_exposure"),
    endur_dataset=Input("/BP/IST-IG-DD/data/technical/exposure/typed/endur/dl130_market_risk_stage_result_summary"),
    power_dataset=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m01015"),
    gas_dataset=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_gas_m0145")
)
def my_compute_function(output_dataset, tr_dataset, endur_dataset, power_dataset, gas_dataset):
    df_tr = tr_dataset.dataframe()
    df_endur = endur_dataset.dataframe()
    df_power = power_dataset.dataframe()
    df_gas = gas_dataset.dataframe()

    # titan alterations
    df_tr = df_tr.groupby(df_tr.RunDateTime, df_tr.Current_COB_Date).agg(F.count(F.lit(1)).alias("Row_Count"))
    df_tr = df_tr.withColumn("File_Name", F.lit("tr200_exposure"))
    df_tr = df_tr.select(df_tr.File_Name, F.current_timestamp().alias("Creation_DateTime"), df_tr.RunDateTime.alias(
        "Run_Datetime"), df_tr.Current_COB_Date.alias("Valuation_Date"), df_tr.Row_Count)

    # endur alterations
    df_endur = df_endur.groupby(df_endur.Run_Datetime, df_endur.Analysis_Date).agg(F.count(F.lit(1)).alias("Row_Count"))
    df_endur = df_endur.withColumn("File_Name", F.lit("dl130_market_risk_stage_result_summary"))
    df_endur = df_endur.select(df_endur.File_Name, F.current_timestamp().alias("Creation_DateTime"), df_endur.Run_Datetime,
                               df_endur.Analysis_Date.alias("Valuation_Date"), df_endur.Row_Count)

    # power alterations
    df_power = df_power.groupby(df_power.Run_Datetime, df_power.PLEX_Calculation_Date).agg(
        F.count(F.lit(1)).alias("Row_Count"))
    df_power = df_power.withColumn("File_Name", F.lit("fp_power_m01015"))
    df_power = df_power.select(df_power.File_Name, F.current_timestamp().alias("Creation_DateTime"), df_power.Run_Datetime,
                               df_power.PLEX_Calculation_Date.alias("Valuation_Date"), df_power.Row_Count)

    # gas alterations
    df_gas = df_gas.groupby(df_gas.Run_Datetime, df_gas.Analysis_Date).agg(
        F.count(F.lit(1)).alias("Row_Count"))
    df_gas = df_gas.withColumn("File_Name", F.lit("fp_gas_m0145"))
    df_gas = df_gas.select(df_gas.File_Name, F.current_timestamp().alias("Creation_DateTime"), df_gas.Run_Datetime,
                           df_gas.Analysis_Date.alias("Valuation_Date"), df_gas.Row_Count)

    # Union
    df = df_tr
    df = df.union(df_endur)
    df = df.union(df_gas)
    df = df.union(df_power)

    output_dataset.write_dataframe(df)
