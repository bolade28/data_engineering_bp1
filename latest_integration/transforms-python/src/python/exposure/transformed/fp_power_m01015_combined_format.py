from transforms.api import transform_df, Input, Output
from pyspark.sql import functions


def convert(df, df_nagp_curves):
    df_nagp_curves = functions.broadcast(
        df_nagp_curves.select(
            df_nagp_curves["Source_Curve_Name"].alias("Nagp_Source_Curve_Name"),
            df_nagp_curves["Curve_Name"].alias("Nagp_Curve_Name"),
            df_nagp_curves["Fobus_Curve_Name"].alias("Nagp_Fobus_Curve_Name"),
            df_nagp_curves["Limits_Group"].alias("Nagp_Limits_Group")
            ))
    df = df.join(df_nagp_curves, df["Curve_Name"] == df_nagp_curves["Nagp_Source_Curve_Name"], "left")\
        .withColumn("Physical_Financial", functions.when(df["Trade_Type_Code"] == "PFWD", "Physical").otherwise(functions.lit("")))\
        .withColumn("Cargo_Reference", functions.when(df["Trade_Type_Code"] == "PFWD", "Physical").otherwise(df["Trade_Type_Code"]))\
        .withColumn("Physical_Book", functions.when(df["Trade_Type_Code"] == "PFWD", "Freeport Power Hedge").otherwise(functions.lit("")))\
        .withColumn("PH", functions.when(df["Trade_Type_Code"] == "PFWD", "Yes").otherwise(functions.lit("")))\
        .withColumn("Exposure_Type", functions.when(df["Trade_Type_Code"] == "PFWD", "Curve").otherwise(functions.lit("")))\
        .withColumn("Trade_Status", functions.when(df["Delivery_From_Period"] > df["Valuation_Date"], "Active").otherwise(functions.lit("")))\
        .withColumn("Fixed_Float", functions.when(df["Delivery_From_Period"] > df["Valuation_Date"], "FLoat").otherwise(functions.lit("")))\
        .withColumn("Pricing_Maturity_Start", functions.date_trunc("month", df["Delivery_From_Period"]))\
        .withColumn("Pricing_Maturity_End", functions.date_trunc("month", df["Delivery_From_Period"]))\
        .withColumn("Pricing_Maturity_Period", functions.date_format(df["Delivery_From_Period"], "MMM-yyyy"))\
        .withColumn("Pricing_Maturity_QTR", functions.concat(
            functions.date_format(df["Delivery_From_Period"], "yyyy"),
            functions.lit(" Q"),
            functions.quarter(df["Delivery_From_Period"])
            ))\
        .withColumn("Pricing_Maturity_Year", functions.date_format(df["Delivery_From_Period"], "yyyy"))\
        .withColumn("Trade_Type", functions.when(df["Trade_Type_Code"] == "PFWD", "Physical").otherwise(functions.lit("")))
    df = df.select(
        functions.lit("Epsilon").alias("Source"),
        df["Valuation_Date"],
        df["Portfolio"],
        df["Cargo_Reference"],
        df["Curve_Name"].alias("Source_Curve_Name"),
        df["Nagp_Curve_Name"].alias("Curve_Name"),
        functions.lit("NON LNG TAC").alias("Trader_Curve_Type"),
        functions.lit("").alias("Curve_Type"),
        functions.lit("Epsilon").alias("Fobus_Report"),
        df["Nagp_Fobus_Curve_Name"].alias("Fobus_Curve_Name"),
        df["Nagp_Limits_Group"].alias("Limits_Group"),
        functions.lit("").alias("GridPoint"),
        df["Book"],
        df["Physical_Financial"],
        df["Physical_Book"],
        functions.lit("").alias("Financial_Book"),
        df["PH"],
        functions.lit("").alias("Paper_BM"),
        df["Counterparty"].alias("Internal_BU"),
        functions.lit("").alias("Counterparty"),
        df["Currency"].alias("Reporting_Currency"),
        functions.lit("Power").alias("Product_Type"),
        functions.lit("Undiscounted").alias("Delta_Type"),
        functions.lit("Curve Price Commodity UOM").alias("Delta_Unit_Type"),
        df["Exposure_Type"],
        functions.to_date(df["Pricing_Maturity_Start"], "yyyy-MM-dd").alias("Pricing_Maturity_Start"),
        functions.to_date(df["Pricing_Maturity_End"], "yyyy-MM-dd").alias("Pricing_Maturity_End"),
        df["Pricing_Maturity_Period"],
        df["Pricing_Maturity_QTR"],
        df["Pricing_Maturity_Year"],
        functions.lit(1.0).alias("Conversion_Rate"),
        df["Hedge_Qty_Current"].alias("Unconformed_Curve_Delta"),
        df["UoM"].alias("Unconformed_Curve_Delta_UOM"),
        df["Hedge_Qty_Current"].alias("Curve_Delta"),
        functions.lit("MWh").alias("Curve_Delta_UOM"),
        df["Price"],
        functions.lit("MWh").alias("Price_UOM"),
        df["Trade_Status"],
        functions.lit("").alias("Fixed_Float"),
        functions.lit("").alias("Delivery_Month"),
        functions.lit("").alias("Delivery_Year"),
        df["Hedge_Qty_Current"].alias("Delta"),
        functions.lit(0.0).alias("Gamma"),
        functions.lit(0.0).alias("Vega"),
        functions.lit("yes").alias("Exposure"),
        df["Trade_Date"],
        df["Trade_ID"],
        df["Trade_Type"],
        df["Commodity"],
        df["Run_Datetime"],
        df["Counterparty"].alias("Legal_Entity_Name"),
        functions.lit("").alias("Titan_Strategy"),
        functions.lit("").alias("Liquid_Window")
    )
    return df


@transform_df(
        Output("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m01015_combined_format"),
        df=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m01015_cleansed"),
        df_ref_nagp_curves=Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Curves"),
)
def myComputeFunction(
        df,	df_ref_nagp_curves
        ):
    return convert(df, df_ref_nagp_curves)
