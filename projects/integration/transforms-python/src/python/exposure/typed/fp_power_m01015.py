from transforms.api import transform_df, incremental, Input, Output
import pyspark.sql.functions as F


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m01015"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_power_m01015/fp_power_m01015"),
)
def my_compute_function(df):
    # TODO: Reconsider how we handle this case
    # We have empty rows in raw file - fillna to prevent error on cast
    # df = df.fillna(0.0, subset=["Valuation_Price"])

    # df = df.na.drop(thresh=len(df.columns)) # garbage rows are blank in all cols except Disc_MTM_Amt_Current
    df = df.na.drop(how='all', subset=["Valuation_Price"])
    df = df.select(
        F.to_date(df.PLEX_Calculation_Date, 'yyyy/MM/dd').alias('PLEX_Calculation_Date'),
        df.Team_Name,
        df.Book_Name,
        df.Counterparty_Name,
        df.Curve_Short_Name,
        df['Notional_Qty_-_Current'].cast('double').alias('Notional_Qty_Current'),
        F.to_date(df.Trade_Date).alias('Trade_Date'),
        df.Trade_ID.cast('long').alias('Trade_ID'),
        df.Commodity_Name,
        df.Trade_Type_Code,
        F.to_date(df.Delivery_From_Period_Summary).alias('Delivery_From_Period_Summary'),
        df.Valuation_Price.cast('double'),
        df['MTM_Price_-_Current'].cast('double').alias('MTM_Price_Current'),
        df['MTM_Amt_-_Current'].cast('double').alias('MTM_Amt_Current'),
        df['Disc_MTM_Amt_-_Current'].cast('double').alias('Disc_MTM_Amt_Current'),
        df['Hedge_Qty_-_Current'].cast('double').alias('Hedge_Qty_Current'),
        F.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
    )
    return df
