from transforms.api import transform_df, incremental, Input, Output
from pyspark.sql import functions as f

import logging
logger = logging.getLogger(__name__)


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/typed/endur/dl130_market_risk_stage_result_summary"),
    df=Input(
        "/BP/IST-IG-SS-Systems/data/raw/endur/dl130-market_risk_stage_result_summary/dl130_market_risk_stage_result_summary"),
)
def my_compute_function(df):

    df = df.select(
        f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
        df['Request_ID'].cast('int'),
        f.to_date(df['Analysis_Date'], 'yyyy-MM-dd').alias('Analysis_Date'),
        df['Request_Type'],
        df['Business_Unit'],
        df['Portfolio'],
        df['Internal_BU'],
        df['Currency_ID'],
        df['Instrument_Type'],
        df['Toolset'],
        df['External_Portfolio'],
        df['Projection_Index'],
        df['UOM'],
        df['Grid_Point_ID'].cast('int'),
        df['Grid_Point_Name'],
        f.to_date(df['Grid_Point_Start_Date'], 'yyyy-MM-dd').alias('Grid_Point_Start_Date'),
        f.to_date(df['Grid_Point_End_Date'], 'yyyy-MM-dd').alias('Grid_Point_End_Date'),
        f.regexp_replace('Delta', ',', '').cast('double').alias('Delta'),
        f.regexp_replace('Gamma', ',', '').cast('double').alias('Gamma'),
        df['Vega'].cast('double'),
        df['Theta'].cast('double'),
        df['Risk_Factor_Name'],
        df['Risk_Factor_CCY'],
        df['Risk_Factor_UOM'],
        df['MR_Delta'].cast('double'),
        df['MR_Gamma'].cast('double'),
        df['MR_Vega'].cast('double'),
        df['MR_Theta'].cast('double'),
        df['Market_Value'].cast('double'),
        df['Bucket_Name'],
        df['Bucket_Id'].cast('int'),
        df['Bucket_Order'].cast('int'),
        f.to_date(df['Bucket_Start_Date'], 'yyyy-MM-dd').alias('Bucket_Start_Date'),
        f.to_date(df['Bucket_End_Date'], 'yyyy-MM-dd').alias('Bucket_End_Date'),
        df['Extract_Type'],
        df['Type'],
    )

    return df
