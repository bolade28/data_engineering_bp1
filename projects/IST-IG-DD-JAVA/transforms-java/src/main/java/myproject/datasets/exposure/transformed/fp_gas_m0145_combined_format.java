package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import myproject.datasets.util.BP_ConversionUtil;

import scala.collection.JavaConversions;
import static java.util.Arrays.asList;

public class fp_gas_m0145_combined_format {
    public Dataset<Row> convert(Dataset<Row> df
            , Dataset<Row> df_nagp_curves
            , Dataset<Row> df_uom_conversions
            , Dataset<Row> df_conformed_units
    ) {
        df_nagp_curves = functions.broadcast(
                df_nagp_curves.select(
                        df_nagp_curves.col("Source_Curve_Name").alias("Nagp_Source_Curve_Name"),
                        df_nagp_curves.col("Curve_Name").alias("Nagp_Curve_Name"),
                        df_nagp_curves.col("Fobus_Curve_Name").alias("Nagp_Fobus_Curve_Name"),
                        df_nagp_curves.col("Limits_Group").alias("Nagp_Limits_Group")
                ));


        df = df.join(df_nagp_curves, df.col("Risk_Factor_Name").equalTo(df_nagp_curves.col("Nagp_Source_Curve_Name")), "left")
                .withColumn("Cargo_Reference", functions
                        .when(df.col("Internal_BU").contains("FIN"), "Financial")
                        .when(df.col("Internal_BU").contains("PHYS"), "Physical"))
                .withColumn("Book", df.col("Portfolio"));
        df=	df.withColumn("Physical_Financial", df.col("Cargo_Reference"));

        df=df.withColumn("Financial_Book", functions.when(df.col("Physical_Financial").equalTo("Financial"), df.col("Book")))
                .withColumn("Physical_Book", functions.when(df.col("Physical_Financial").equalTo("Physical"), df.col("Book")))
                .withColumn("PH", functions.when(df.col("Book").endsWith("- PH"), functions.lit("Yes")))
                .withColumn("Exposure_Type", functions.when(df.col("Instrument_Type").equalTo("FX"), "FX").otherwise(functions.lit("Curve")))
                .withColumn("Pricing_Maturity_Start", functions.date_trunc("month", df.col("Grid_Point_End_Date")))
                .withColumn("Pricing_Maturity_End", functions.last_day(df.col("Grid_Point_End_Date")))
                .withColumn("Pricing_Maturity_Period", functions.date_format(df.col("Grid_Point_End_Date"), "MMM-yyyy"))
                .withColumn("Pricing_Maturity_QTR", functions.concat(
                        functions.date_format(df.col("Grid_Point_End_Date"), "yyyy"),
                        functions.lit(" Q"),
                        functions.quarter(df.col("Grid_Point_End_Date"))
                ))
                .withColumn("Pricing_Maturity_Year", functions.date_format(df.col("Grid_Point_End_Date"), "yyyy"))
                .withColumn("Trade_Status", functions.when(df.col("Grid_Point_End_Date").$greater(functions.current_date()), "Active").otherwise(functions.lit("")))
                .withColumn("Fixed_Float", functions.when(df.col("Grid_Point_End_Date").$greater(functions.current_date()), "Float").otherwise(functions.lit("")))
                .withColumn("Exposure", functions.when(df.col("MR_Delta").notEqual(0.0).or(df.col("MR_Delta").notEqual(0.0)).or(df.col("MR_Delta").notEqual(0.0)), functions.lit("yes")))
        ;

        // unit conversion
        df = df.withColumn("Unconformed_Curve_Delta", df.col("MR_Delta"));
        df = df.withColumn("Unconformed_Curve_Delta_UOM", df.col("UOM"));
        
        df = BP_ConversionUtil.addConformedUnit(df, df_conformed_units, "Unconformed_Curve_Delta_UOM", "Curve_Delta_UOM");        

        df = BP_ConversionUtil.addConversionRate(df, df_uom_conversions, "Unconformed_Curve_Delta", "Unconformed_Curve_Delta_UOM", "Curve_Delta_UOM", "Conversion_Rate", true);

        df = df.withColumn("Curve_Delta_UOM",
                functions.when(df.col("Unconformed_Curve_Delta_UOM").equalTo("DAYS MRA"), functions.lit("days"))
                        .when(df.col("Unconformed_Curve_Delta_UOM").equalTo("MWH"), functions.lit("MWh"))
                        .otherwise(df.col("Curve_Delta_UOM"))
        );

        df = df.withColumn("Conversion_Rate",
                functions.when(df.col("Unconformed_Curve_Delta_UOM").isin("DAYS MRA","MWH","EUR","GBP"), functions.lit(1.0))
                .otherwise(df.col("Conversion_Rate"))
        );

        df = df.withColumn("Curve_Delta",  df.col("Unconformed_Curve_Delta").multiply(df.col("Conversion_Rate")))
                .withColumn("Gamma", df.col("MR_Gamma").multiply(df.col("Conversion_Rate")))
                .withColumn("Vega", df.col("MR_Vega").multiply(df.col("Conversion_Rate")));

        df = df.na().fill("Mapping not found", JavaConversions.asScalaBuffer(asList("Nagp_Fobus_Curve_Name")));
 
        df = df.select(
                functions.lit("S3 Gas").alias("Source"),
                df.col("Valuation_Date"),
                df.col("Portfolio"),
                df.col("Cargo_Reference"),
                df.col("Risk_Factor_Name").alias("Source_Curve_Name"),
                df.col("Nagp_Curve_Name").alias("Curve_Name"),
                functions.lit("NON LNG TAC").alias("Trader_Curve_Type"),
                functions.lit("").alias("Curve_Type"),
                functions.lit("S3 Gas").alias("Fobus_Report"),
                df.col("Nagp_Fobus_Curve_Name").alias("Fobus_Curve_Name"),
                df.col("Nagp_Limits_Group").alias("Limits_Group"),
                functions.lit("").alias("GridPoint"),
                df.col("Book"),
                df.col("Physical_Financial"),
                df.col("Physical_Book"),
                functions.lit("").alias("Financial_Book"),
                df.col("PH"),
                functions.lit("").alias("Paper_BM"),
                df.col("Internal_BU").alias("Internal_BU"),
                df.col("Counterparty"),
                df.col("Currency").alias("Reporting_Currency"),
                df.col("Instrument_Type").alias("Product_Type"),
                functions.lit("FVT").alias("Delta_Type"),
                functions.lit("Curve Price Commodity UOM").alias("Delta_Unit_Type"),
                df.col("Exposure_Type"),
                functions.to_date(df.col("Pricing_Maturity_Start"), "yyyy-MM-dd").alias("Pricing_Maturity_Start"),
                functions.to_date(df.col("Pricing_Maturity_End"), "yyyy-MM-dd").alias("Pricing_Maturity_End"),
                df.col("Pricing_Maturity_Period"),
                df.col("Pricing_Maturity_QTR"),
                df.col("Pricing_Maturity_Year"),
                df.col("Conversion_Rate"),
                df.col("Unconformed_Curve_Delta"),
                df.col("Unconformed_Curve_Delta_UOM"),
                df.col("Curve_Delta"),
                df.col("Curve_Delta_UOM"),
                functions.lit(0.0).alias("Price"),
                functions.lit("").alias("Price_UOM"),
                df.col("Trade_Status"),
                df.col("Fixed_Float"),
                functions.lit("").alias("Delivery_Month"),
                functions.lit("").alias("Delivery_Year"),
                df.col("Delta"),
                df.col("Gamma"),
                df.col("Vega"),
                df.col("Exposure"),
                df.col("Trade_Date"),
                df.col("Deal_Tracking_Num").alias("Trade_ID"),
                functions.lit("").alias("Trade_Type"),
                functions.lit("Gas").alias("Commodity"),
                df.col("Run_Datetime"),
                df.col("Legal_Entity_Name"),
                functions.lit("").alias("Titan_Strategy"),
                functions.lit("").alias("Liquid_Window")
        );
        return df;

    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_gas_m0145_combined_format")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_gas_m0145_cleansed") Dataset<Row> df,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions") Dataset<Row> df_ref_uom_conversions,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units") Dataset<Row> df_ref_conformed_units,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Curves") Dataset<Row> df_ref_nagp_curves
    ){
        return convert(
                df
                , df_ref_nagp_curves
                , df_ref_uom_conversions
                , df_ref_conformed_units
        );
    }
}
