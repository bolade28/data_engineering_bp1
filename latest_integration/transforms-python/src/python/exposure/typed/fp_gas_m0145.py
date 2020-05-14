from transforms.api import transform_df, incremental, Input, Output
import pyspark.sql.functions as F


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_gas_m0145"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_gas_m0145/fp_gas_m0145"),
)
def my_compute_function(df):
    """
    Transformation stage function
    Version: T2
    This function:
        For processing raw data and converting to type
        Reorders the dataframe using select statement.
    Args:
        df (dataframe): current fp_gas_145m

    Returns:
        dataframe: transformed fp_gas_145m
    """
    df = df.select(
        df['Request_ID'].cast('int'),
        F.to_date(df['Analysis_Date'], 'dd/MM/yyyy').alias('Analysis_Date'),
        df['Request_Type'],
        df['Contract_Month_DMD'].cast('int'),
        df['Deal_Tracking_Num'].cast('int'),
        df['Business_Unit'],
        df['Trader'],
        df['Portfolio'],
        df['External_BU'],
        df['Internal_BU'],
        df['Currency'],
        df['Instrument_Type'],
        F.to_date(df['Trade_Date'], 'dd/MM/yyyy').alias('Trade_Date'),
        F.to_date(df['Trade_Time'], 'dd/MM/yyyy').alias('Trade_Time'),
        df['Toolset'],
        df['External_Portfolio'],
        df['Broker'],
        df['OTC_Clearing_Broker'].cast('int'),
        df['Offset_Transaction_Number'].cast('int'),
        df['Reference'],
        df['Projection_Index'],
        df['UOM'],
        df['Grid_Point_ID'].cast('int'),
        df['Grid_Point_Name'],
        F.to_date(df['Grid_Point_Start_Date'], 'dd/MM/yyyy').alias('Grid_Point_Start_Date'),
        F.to_date(df['Grid_Point_End_Date'], 'dd/MM/yyyy').alias('Grid_Point_End_Date'),
        F.to_date(df['Grid_Point_Start_Date_Long'], 'dd/MM/yyyy').alias('Grid_Point_Start_Date_Long'),
        F.to_date(df['Grid_Point_End_Date_Long'], 'dd/MM/yyyy').alias('Grid_Point_End_Date_Long'),
        df['Delta'].cast('double'),
        df['Gamma'].cast('double'),
        df['Vega'].cast('double'),
        df['Theta'].cast('double'),
        df['Risk_Factor_Name'],
        df['Delta_Shift'].cast('double'),
        df['Gamma_Factor'].cast('int'),
        df['Risk_Factor_CCY'],
        df['CCY_Conversion_Rate'].cast('double'),
        df['Risk_Factor_UOM'],
        df['UOM_Conversion_Rate'].cast('double'),
        df['MR_Delta'].cast('double'),
        df['MR_Gamma'].cast('double'),
        df['MR_Vega'].cast('double'),
        df['MR_Theta'].cast('double'),
        df['Market_Value'].cast('double'),
        df['Bucket_Name'],
        df['Bucket_Id'].cast('int'),
        df['Bucket_Order'].cast('int'),
        F.to_date(df['Bucket_Start_Date'], 'dd/MM/yyyy').alias('Bucket_Start_Date'),
        F.to_date(df['Bucket_End_Date'], 'dd/MM/yyyy').alias('Bucket_End_Date'),
        df['Extract_Type'],
        df['Type'],
        F.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
    )
    return df
