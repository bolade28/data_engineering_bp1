# ===== import: python function
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.exposure.schema_exposure.schema_tr200_exposure_combined_detail import transformed_output_schema
from python.util.schema_utils import compute_transform, reorder_columns
from python.util.ref_data_error_identification import get_ref_data_status_column
from python.util.exposure_util import add_grid_point_month


def convert(df,
            df_ref_titan_curve,
            df_ref_titan_conv,
            df_ref_uom_conversions,
            df_ref_conformed_units,
            df_ref_portfolio
            ):
    df_ref_titan_curve = f.broadcast(df_ref_titan_curve.withColumnRenamed("Curve_Name", "Titan_Curve_Name")
                                     .withColumnRenamed("Start_Date", "Titan_Curve_Start_Date")
                                     .withColumnRenamed("End_Date", "Titan_Curve_End_Date")
                                     .withColumnRenamed("Last_Updated_Timestamp", "Titan_Curve_Last_Updated")
                                     .withColumnRenamed("Last_Updated_User", "Titan_Curve_Last_Updated_User"))

    df_ref_titan_conv = f.broadcast(df_ref_titan_conv
                                    .withColumnRenamed("Conversion", "Titan_Oil_Conversion_Rate")
                                    .withColumnRenamed("Curve_Name", "Titan_Oil_Curve_Name")
                                    .withColumnRenamed("Start_Date", "Titan_Oil_Start_Date")
                                    .withColumnRenamed("End_Date", "Titan_Oil_End_Date")
                                    .withColumnRenamed("Last_Updated_Timestamp", "Titan_Oil_Last_Updated")
                                    .withColumnRenamed("Last_Updated_User", "Titan_Oil_Last_Updated_User"))

    df_ref_uom_conversions = f.broadcast(df_ref_uom_conversions
                                         .withColumnRenamed("From_Unit", "Uom_From_Unit")
                                         .withColumnRenamed("To_Unit", "Uom_To_Unit")
                                         .withColumnRenamed("Conversion", "Uom_Conversion_Rate")
                                         .withColumnRenamed("Start_Date", "Uom_Start_Date")
                                         .withColumnRenamed("End_Date", "Uom_End_Date")
                                         .withColumnRenamed("Last_Updated_Timestamp", "Uom_Last_Updated")
                                         .withColumnRenamed("Last_Updated_User", "Uom_Last_Updated_User"))

    df_ref_conformed_units = f.broadcast(df_ref_conformed_units)
    df = df.join(df_ref_titan_curve, df["Curve_Name"] == df_ref_titan_curve["Source_Curve_Name"], "left")
    df = df.join(df_ref_titan_conv, df["Titan_Curve_Name"] == df_ref_titan_conv["Titan_Oil_Curve_Name"], "left")
    df = df.join(df_ref_conformed_units.filter(
        df_ref_conformed_units["BU"] == "LNG"), df["Curve_Delta_UOM"] == df_ref_conformed_units["From_Unit"], "left")

    df = df.join(f.broadcast(df_ref_uom_conversions), (df_ref_uom_conversions.Uom_From_Unit == df.Curve_Delta_UOM) &
                 (df_ref_uom_conversions.Uom_To_Unit == df.Conformed_Unit), "left")

    df_uom_conversions = df_ref_uom_conversions.groupBy("Uom_From_Unit").pivot("Uom_To_Unit").sum("Uom_Conversion_Rate")
    df_uom_conversions = df_uom_conversions.withColumnRenamed("Uom_From_Unit", "From_Unit_Conversion")
    df_uom_conversions = df_uom_conversions.select('From_Unit_Conversion', 'BBL', 'MWh', 'MMBtu', 'Therms')
    df = df.join(f.broadcast(df_uom_conversions),
                 df_uom_conversions["From_Unit_Conversion"] == df["Curve_Delta_UOM"], "left")

    df = df.withColumn("MR_Delta_Conformed_UOM",
                       f.when(df["Curve_Delta_UOM"].isin("day", "Percentage",
                                                         "EUR", "GBP", "USD"), df["Curve_Delta_UOM"])
                       .when(df["Curve_Name"] == "JLC", "BBL")
                       .when(df["Curve_Delta_UOM"].isin("MT", "Tonnes") &
                             df["Titan_Oil_Conversion_Rate"].isNotNull(), "BBL")
                       .when(df["Curve_Delta_UOM"].isin("MT", "Tonnes") &
                             df["Titan_Oil_Conversion_Rate"].isNull(), df["Curve_Delta_UOM"])
                       .when(df["Uom_Conversion_Rate"].isNotNull(), df["Conformed_Unit"])
                       .otherwise(df["Curve_Delta_UOM"])
                       )
    df = df.withColumn("Conversion_Rate",
                       f.when(df["Curve_Delta_UOM"].isin("day", "Percentage", "EUR", "GBP", "USD"), 1.0)
                       .when(df["Curve_Name"] == "JLC", 1.0)
                       .when(df["Curve_Delta_UOM"].isin("MT", "Tonnes") &
                             df["Titan_Oil_Conversion_Rate"].isNotNull(), df["Titan_Oil_Conversion_Rate"])
                       .when(df["Curve_Delta_UOM"].isin("MT", "Tonnes") &
                             df["Titan_Oil_Conversion_Rate"].isNull(), 1.0)
                       .when(df["Uom_Conversion_Rate"].isNotNull(), df["Uom_Conversion_Rate"])
                       .otherwise(f.lit(1.0))
                       )
    df = df.withColumn("MR_Delta_Conformed", (df["Curve_Delta"] * df["Conversion_Rate"]))
    df = df.withColumn("Bucket_Month", f.date_format(df["Pricing_Maturity_End"], "MMM-yyyy"))
    df = df.withColumn("Bucket_Quarter", f.concat(
                f.date_format(df["Pricing_Maturity_End"], "yyyy"),
                f.lit(" Q"),
                f.quarter(df["Pricing_Maturity_End"])
            ))
    df = df.withColumn("Bucket_Year", f.date_format(df["Pricing_Maturity_End"], "yyyy"))
    df = df.withColumn("Source", f.lit("Titan"))
    df = df.withColumn("MR_Gamma", f.lit(0))
    df = df.withColumn("MR_Vega", f.lit(0))
    df = df.withColumn("MR_Theta", f.lit(0))
    df = df.withColumn("Market_Value", f.lit(0))
    df = df.withColumn("Bucket_Name", f.lit(None).cast(StringType()))
    df = df.withColumn("Bucket_Id", f.lit(None).cast(StringType()))
    df = df.withColumn("Bucket_Order", f.lit(None).cast(StringType()))
    df = df.withColumn("Grid_Point_ID", f.lit(None).cast(StringType()))
    df = df.withColumn("Grid_Point_Name", f.lit(None).cast(StringType()))
    df = df.withColumn("Grid_Point_Start_Date", f.lit(None).cast(StringType()))
    df = df.withColumn("Grid_Point_End_Date", f.lit(None).cast(StringType()))
    df = add_grid_point_month(df)
    df = df.withColumn("Internal_BU", f.lit(None).cast(StringType()))
    df = df.withColumn("External_BU", f.lit(None).cast(StringType()))
    df = df.withColumn("External_Portfolio", f.lit(None).cast(StringType()))
    df = df.withColumn("Instrument_Type", f.lit(None).cast(StringType()))
    df = df.withColumn("Expo_Type", f.lit(None).cast(StringType()))
    df = df.withColumn("Deal_Trader", f.lit(None).cast(StringType()))

    df = df.withColumn("Risk_Factor_Name", df["Titan_Curve_Name"])
    # df = df.na.fill({"Risk_Factor_Name": "unmapped"})
    df = df.withColumn("Limits_Group", df["Limits_Group"])
    # df = df.na.fill({"Limits_Group": "unmapped"})

    df = df.withColumn("MR_Delta", f.round(df["Curve_Delta"], 0))
    df = df.withColumn("MR_Delta_Conformed",  f.round(df["MR_Delta_Conformed"], 0))

    df = df.withColumn("MR_Delta_Therms",
                       f.when(df["Curve_Delta_UOM"] == "Th", df["MR_Delta"])
                        .when((df["Curve_Delta_UOM"] == "MMBtu") | (df["Curve_Delta_UOM"] == "MWh"),
                              f.round(df["MR_Delta"] * df["Therms"], 0)))

    df = df.withColumn("MR_Delta_MWh",
                       f.when(df["Curve_Delta_UOM"] == "MWh", df["MR_Delta"])
                        .when((df["Curve_Delta_UOM"] == "MMBtu") | (df["Curve_Delta_UOM"] == "Th"),
                              f.round(df["MR_Delta"] * df["MWh"], 0)))

    df = df.withColumn("MR_Delta_MMBtu",
                       f.when(df["Curve_Delta_UOM"] == "MMBtu", df["MR_Delta"])
                        .when((df["Curve_Delta_UOM"] == "Th") | (df["Curve_Delta_UOM"] == "MWh"),
                              f.round(df["MR_Delta"] * df["MMBtu"], 0)))

    df = df.withColumn("MR_Delta_BBL",
                       f.when(df["Curve_Delta_UOM"] == "bbl", df["MR_Delta"])
                        .when((df["Curve_Delta_UOM"] == "Th") | (df["Curve_Delta_UOM"] == "Tonnes"),
                              f.round(df["MR_Delta"] * df["Titan_Oil_Conversion_Rate"], 0)))

    df = df.na.fill({"MR_Delta_Therms": 0})
    df = df.na.fill({"MR_Delta_MWh": 0})
    df = df.na.fill({"MR_Delta_MMBtu": 0})
    df = df.na.fill({"MR_Delta_BBL": 0})

    df = df.withColumn('MR_Delta_EUR', f.when((f.col('Curve_Delta_UOM') == 'EUR'), f.col('MR_Delta'))
                                        .otherwise(f.lit(0)))

    df = df.withColumn('MR_Delta_GBP', f.when((f.col('Curve_Delta_UOM') == 'GBP'), f.col('MR_Delta'))
                                        .otherwise(f.lit(0)))

    df = df.withColumn("Cargo_Delivery_Month", f.date_format(df["Delivery_End_Date"], "MM"))
    df = df.withColumn("Cargo_Delivery_Year", f.date_format(df["Delivery_End_Date"], "yyyy"))

    df = df.withColumn("Liquid_Window", f.lit("Yes"))

    # April 2019 to September 2019 = 2019 S, October 2019 - March 2020 = 2019 W (S = summer, W = winter)
    df = df.withColumn('Bucket_Season',
                       f.lit(
                        f.concat(
                            (f.date_format(f.add_months(df['Pricing_Maturity_End'], -3), 'yyyy ')),
                            f.when(
                                f.month(f.add_months(df['Pricing_Maturity_End'], -3)) > 6,
                                f.lit("W")
                            )
                            .otherwise(f.lit("S"))
                         )
                        )
                       )
    df = df.withColumn("RefData_Datetime", f.greatest("Titan_Curve_Last_Updated", "Titan_Oil_Last_Updated",
                                                      "Uom_Last_Updated", "Last_Updated_Timestamp"))

    df_res = compute_transform(df, transformed_output_schema)

    ref_data = (df_ref_portfolio.withColumnRenamed("Portfolio", "Ref_Portfolio")
                .withColumnRenamed("Last_Updated_Timestamp", "Refdata_Datetime")
    )
    ref_add_cols = ['Bench', 'Team', 'Strategy', 'Trader']
    df_res = get_ref_data_status_column(df_res, ref_data, 'Portfolio_Attribute',
                                    ref_add_cols, 
                                    'Ref_Portfolio',
                                    'Refdata_Datetime',
                                    portfolio_col= 'Portfolio/Book')

    # Reorder according to business requirements
    df_res = reorder_columns(df_res, transformed_output_schema)
    return df_res


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/transformed/tr200_exposure_combined_detail"),
    df_titan_input=Input("/BP/IST-IG-DD/data/technical/exposure/current/titan/tr200_exposure"),
    df_ref_titan_curve=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Titan_Curves"),
    df_ref_titan_conv=Input('/BP/IST-IG-UC-User-Data/data/referencedata/Titan_Oil_UOM_Conversions'),
    df_ref_uom_conversions=Input('/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions'),
    df_ref_conformed_units=Input('/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units'),
    df_ref_portfolio=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes"),
)
def myComputeFunction(
        df_titan_input,	df_ref_titan_curve,	df_ref_titan_conv,	df_ref_uom_conversions,	df_ref_conformed_units, df_ref_portfolio
        ):
    df_titan_output = convert(
        df_titan_input,
        df_ref_titan_curve,
        df_ref_titan_conv,
        df_ref_uom_conversions,
        df_ref_conformed_units,
        df_ref_portfolio)

    return df_titan_output
