package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class fp_power_m01015_combined_format {
    public Dataset<Row> convert(Dataset<Row> df, Dataset<Row> df_nagp_curves )
    {
        df_nagp_curves = functions.broadcast(
                df_nagp_curves.select(
                        df_nagp_curves.col("Source_Curve_Name").alias("Nagp_Source_Curve_Name"),
                        df_nagp_curves.col("Curve_Name").alias("Nagp_Curve_Name"),
                        df_nagp_curves.col("Fobus_Curve_Name").alias("Nagp_Fobus_Curve_Name"),
                        df_nagp_curves.col("Limits_Group").alias("Nagp_Limits_Group")
        ));

        df = df.join(df_nagp_curves, df.col("Curve_Name").equalTo(df_nagp_curves.col("Nagp_Source_Curve_Name")), "left")
                .withColumn("Physical_Financial", functions.when(df.col("Trade_Type_Code").equalTo("PFWD"), "Physical").otherwise(functions.lit("")))
                .withColumn("Cargo_Reference", functions.when(df.col("Trade_Type_Code").equalTo("PFWD"), "Physical").otherwise(df.col("Trade_Type_Code")))
                .withColumn("Physical_Book", functions.when(df.col("Trade_Type_Code").equalTo("PFWD"), "Freeport Power Hedge").otherwise(functions.lit("")))
                .withColumn("PH", functions.when(df.col("Trade_Type_Code").equalTo("PFWD"), "Yes").otherwise(functions.lit("")))
                .withColumn("Exposure_Type", functions.when(df.col("Trade_Type_Code").equalTo("PFWD"), "Curve").otherwise(functions.lit("")))
                .withColumn("Trade_Status", functions.when(df.col("Delivery_From_Period").$greater(df.col("Valuation_Date")), "Active").otherwise(functions.lit("")))
                .withColumn("Fixed_Float", functions.when(df.col("Delivery_From_Period").$greater(df.col("Valuation_Date")), "FLoat").otherwise(functions.lit("")))
                .withColumn("Pricing_Maturity_Start", functions.date_trunc("month", df.col("Delivery_From_Period")))
                .withColumn("Pricing_Maturity_End", functions.date_trunc("month", df.col("Delivery_From_Period")))
                .withColumn("Pricing_Maturity_Period", functions.date_format(df.col("Delivery_From_Period"), "MMM-yyyy"))
                .withColumn("Pricing_Maturity_QTR", functions.concat(
                        functions.date_format(df.col("Delivery_From_Period"), "yyyy"),
                        functions.lit(" Q"),
                        functions.quarter(df.col("Delivery_From_Period"))
                ))
                .withColumn("Pricing_Maturity_Year", functions.date_format(df.col("Delivery_From_Period"), "yyyy"))
                .withColumn("Trade_Type", functions.when(df.col("Trade_Type_Code").equalTo("PFWD"), "Physical").otherwise(functions.lit("")));

        df = df.select(
                functions.lit("Epsilon").alias("Source"),
                df.col("Valuation_Date"),
                df.col("Portfolio"),
                df.col("Cargo_Reference"),
                df.col("Curve_Name").alias("Source_Curve_Name"),
                df.col("Nagp_Curve_Name").alias("Curve_Name"),
                functions.lit("NON LNG TAC").alias("Trader_Curve_Type"),
                functions.lit("").alias("Curve_Type"),
                functions.lit("Epsilon").alias("Fobus_Report"),
                df.col("Nagp_Fobus_Curve_Name").alias("Fobus_Curve_Name"),
                df.col("Nagp_Limits_Group").alias("Limits_Group"),
                functions.lit("").alias("GridPoint"),
                df.col("Book"),
                df.col("Physical_Financial"),
                df.col("Physical_Book"),
                functions.lit("").alias("Financial_Book"),
                df.col("PH"),
                functions.lit("").alias("Paper_BM"),
                df.col("Counterparty").alias("Internal_BU"),
                functions.lit("").alias("Counterparty"),
                df.col("Currency").alias("Reporting_Currency"),
                functions.lit("Power").alias("Product_Type"),
                functions.lit("Undiscounted").alias("Delta_Type"),
                functions.lit("Curve Price Commodity UOM").alias("Delta_Unit_Type"),
                df.col("Exposure_Type"),
                functions.to_date(df.col("Pricing_Maturity_Start"), "yyyy-MM-dd").alias("Pricing_Maturity_Start"),
                functions.to_date(df.col("Pricing_Maturity_End"), "yyyy-MM-dd").alias("Pricing_Maturity_End"),
                df.col("Pricing_Maturity_Period"),
                df.col("Pricing_Maturity_QTR"),
                df.col("Pricing_Maturity_Year"),
                functions.lit(1.0).alias("Conversion_Rate"),
                df.col("Hedge_Qty_Current").alias("Unconformed_Curve_Delta"),
                df.col("UoM").alias("Unconformed_Curve_Delta_UOM"),
                df.col("Hedge_Qty_Current").alias("Curve_Delta"),
                functions.lit("MWh").alias("Curve_Delta_UOM"),
                df.col("Price"),
                functions.lit("MWh").alias("Price_UOM"),
                df.col("Trade_Status"),
                functions.lit("").alias("Fixed_Float"),
                functions.lit("").alias("Delivery_Month"),
                functions.lit("").alias("Delivery_Year"),
                df.col("Hedge_Qty_Current").alias("Delta"),
                functions.lit(0.0).alias("Gamma"),
                functions.lit(0.0).alias("Vega"),
                functions.lit("yes").alias("Exposure"),
                df.col("Trade_Date"),
                df.col("Trade_ID"),
                df.col("Trade_Type"),
                df.col("Commodity"),
                df.col("Run_Datetime"),
                df.col("Counterparty").alias("Legal_Entity_Name"),
                functions.lit("").alias("Titan_Strategy"),
                functions.lit("").alias("Liquid_Window")
        );


        return df;
    }
    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_power_m01015_combined_format")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_power_m01015_cleansed") Dataset<Row> df,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Curves") Dataset<Row> df_ref_nagp_curves
    ){
        return convert(df, df_ref_nagp_curves);
    }
}
