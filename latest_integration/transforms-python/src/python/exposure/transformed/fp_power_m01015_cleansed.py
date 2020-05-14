from transforms.api import transform_df, Input, Output
from pyspark.sql import functions


def convert(df):
    df = df.withColumnRenamed("PLEX_Calculation_Date", "Valuation_Date")\
            .withColumnRenamed("Team_Name", "Portfolio")\
            .withColumnRenamed("Book_Name", "Book")\
            .withColumnRenamed("Counterparty_Name", "Counterparty")\
            .withColumnRenamed("Curve_Short_Name", "Curve_Name")\
            .withColumnRenamed("Commodity_Name", "Commodity")\
            .withColumnRenamed("Delivery_From_Period_Summary", "Delivery_From_Period")\
            .withColumnRenamed("Valuation_Price", "Price")\
            .withColumn("UoM",
                        functions.when(df["Trade_Type_Code"].isNull(), "Unknown")
                        .when(df["Trade_Type_Code"] == "PFWD", functions.lit("MWh"))
                        .when(df["Trade_Type_Code"] == "FX_EUR", functions.lit("EUR"))
                        .when(df["Trade_Type_Code"] == "FX_GBP", functions.lit("GBP"))
                        )\
        .withColumn("Currency", functions.lit("USD"))
    return df


@transform_df(
        Output("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m01015_cleansed"),
        df=Input("/BP/IST-IG-DD/data/technical/exposure/current/freeport/fp_power_m01015"),
)
def myComputeFunction(df):
    return convert(df)
