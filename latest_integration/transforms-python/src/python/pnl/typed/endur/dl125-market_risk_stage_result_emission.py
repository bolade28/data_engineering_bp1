# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, incremental

# ===== import: our functions


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl125-market_risk_stage_result_emission"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl125-market_risk_stage_result_emission/dl125-market_risk_stage_result_emission"),
)
def my_compute_function(df):
    '''
    Version: V0
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        params1 (dataframe): raw dl125

    Returns:
        dataframe: typed dl125
    '''
    df = df.select(
        f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
        df['Request_ID'].cast('int'),
        f.to_date(df['Analysis_Date'], 'yyyy-MM-dd').alias('Analysis_Date'),
        df['Request_Type'],
        df['Deal_Tracking_Num'].cast('int'),
        df['Business_Unit'],
        df['Trader'],
        df['Portfolio'],
        df['External_BU'],
        df['Internal_BU'],
        df['Currency_ID'],
        df['Instrument_Type'],
        f.to_date(df['Trade_Date'], 'yyyy-MM-dd').alias('Trade_Date'),
        f.to_timestamp(df['Trade_Time'], 'yyyy-MM-dd HH:mm:ss').alias('Trade_Time'),
        df['Toolset'],
        df['External_Portfolio'],
        df['Broker'],
        df['OTC_Clearing_Broker'],
        df['Offset_Transaction_Number'].cast('int'),
        df['Reference'],
        df['Projection_Index'],
        df['UOM'],
        df['Grid_Point_ID'].cast('int'),
        df['Grid_Point_Name'],
        f.to_date(df['Grid_Point_Start_Date'], 'yyyy-MM-dd').alias('Grid_Point_Start_Date'),
        f.to_date(df['Grid_Point_End_Date'], 'yyyy-MM-dd').alias('Grid_Point_End_Date'),
        f.to_date(df['Grid_Point_Start_Date_Long'], 'yyyy-MM-dd').alias('Grid_Point_Start_Date_Long'),
        f.to_date(df['Grid_Point_End_Date_Long'], 'yyyy-MM-dd').alias('Grid_Point_End_Date_Long'),
        df['Delta'],
        df['Gamma'].cast('double'),
        df['Vega'],
        df['Theta'],
        df['Risk_Factor_Name'],
        df['Delta_Shift'].cast('double'),
        df['Gamma_Factor'].cast('double'),
        df['Risk_Factor_CCY'],
        df['CCY_Conversion_Rate'].cast('double'),
        df['Risk_Factor_UOM'],
        df['UOM_Conversion_Rate'].cast('double'),
        df['MR_Delta'].cast('double'),
        df['MR_Gamma'].cast('double'),
        df['MR_Vega'].cast('double'),
        df['MR_Theta'].cast('double'),
        df['Market_Value'].cast('double'),
        df['Bucket_Name'].cast('int'),
        df['Bucket_Id'].cast('int'),
        df['Bucket_Order'].cast('int'),
        f.to_date(df['Bucket_Start_Date'], 'yyyy-MM-dd').alias('Bucket_Start_Date'),
        f.to_date(df['Bucket_End_Date'], 'yyyy-MM-dd').alias('Bucket_End_Date'),
        df['Extract_Type'],
    )
    return df
