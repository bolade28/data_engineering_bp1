package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import myproject.datasets.util.BP_ConversionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import scala.collection.Seq;
import scala.collection.JavaConverters;

import java.util.Date;
import java.util.*;

public class dl130_Endur_Monthly_Exposure_combined_format {
    public Dataset<Row> convert(Dataset<Row> df,
                                Dataset<Row> df_endur_option_curves,
                                Dataset<Row> df_endur_curves,
                                Dataset<Row> df_uom_conversions,
                                Dataset<Row> df_conformed_units
    ) {
        df = df.filter(df.col("Internal_BU").startsWith("LNG"));
        
        df_endur_option_curves = functions.broadcast(
                df_endur_option_curves.withColumnRenamed("Instrument_Type", "Endur_Option_Curves_Instrument_Type")
                .withColumnRenamed("Display_Name", "Endur_Option_Curves_Display_Name")
                );
        
        df_endur_curves = functions.broadcast(
                df_endur_curves.withColumnRenamed("Source_Curve_Name", "Endur_Curves_Source_Curve_Name")
                                .withColumnRenamed("Curve_Name", "Endur_Curves_Curve_Name")
                );

        //Source_Curve_Name
        df = df.withColumn("Source_Curve_Name", functions.coalesce(df.col("Projection_Index"), df.col("Risk_Factor_Name")));
        
        // Instrument_Type --- get Endur_Option_Curves_Diplay_Name from df_endur_option_curves
        df = df.join(df_endur_option_curves, df.col("Instrument_Type").equalTo(df_endur_option_curves.col("Endur_Option_Curves_Instrument_Type"))
                        .and(df.col("Risk_Factor_Name").equalTo(df_endur_option_curves.col("Risk_Factor"))), 
                        "left")
                        .drop("")
                        ;
        // Instrument_Type --- get Endur_Curves_Curve_Name from df_endur_curves
        df = df.join(df_endur_curves, df.col("Source_Curve_Name").equalTo(df_endur_curves.col("Endur_Curves_Source_Curve_Name")), "left");

        df  = df.withColumn("Curve_Name", functions.coalesce(df.col("Endur_Option_Curves_Display_Name"), df.col("Endur_Curves_Curve_Name")));

        // unit conversion
        df = df.withColumn("Unconformed_Curve_Delta", df.col("MR_Delta_Base_UOM"));
        df = df.withColumn("Unconformed_Curve_Delta_UOM", df.col("UOM"));
        
        df = BP_ConversionUtil.addConformedUnit(df, df_conformed_units, "Unconformed_Curve_Delta_UOM", "Curve_Delta_UOM");

        df = BP_ConversionUtil.addConversionRate(df, df_uom_conversions, "Unconformed_Curve_Delta", "Unconformed_Curve_Delta_UOM", "Curve_Delta_UOM", "Conversion_Rate", true);


        // 
        df = df.withColumn("Fobus_Curve_Name", functions.when((functions.isnull(functions.col("Fobus"))).or
                        (functions.length(functions.col("Fobus")).equalTo(0)),
                functions.col("Fobus_Curve_Name")
        ).otherwise(functions.col("Fobus")));

        df = df.withColumn("limit_temp", functions.when((functions.isnull(functions.col("Limits"))).or
                        (functions.length(functions.col("Limits")).equalTo(0)),
                functions.col("Limits_Group")
        ).otherwise(functions.col("Limits")));

        df = df.withColumn("phys_fin_temp", functions.when(functions.col("Portfolio").contains("FIN"), "Financial")
                .when(functions.col("Portfolio").contains("PHYS"), "Physical"));

        df = df.withColumn("fin_book_temp", functions.when(functions.lit("Financial").equalTo(functions.col("phys_fin_temp")),
                functions.col("Portfolio")));

        df = df.withColumn("phys_book_temp", functions.when(functions.lit("Physical").equalTo(functions.col("phys_fin_temp")),
                functions.col("Portfolio")));

        df = df.withColumn("Pricing_Maturity_Start", functions.to_date(functions.date_trunc("month", df.col("Bucket_Start_Date")), "yyyy-MM-dd"))
                .withColumn("Pricing_Maturity_End", functions.last_day(df.col("Bucket_End_Date")))
                .withColumn("Pricing_Maturity_Period",df.col("Bucket_Month"));


        df = df.withColumn("Pricing_Maturity_QTR", functions.concat(functions.year(functions.col("Bucket_End_Date")).cast("string"),
                functions.lit(" Q"), functions.quarter(functions.col("Bucket_End_Date"))));

        df = df.withColumn("Pricing_Maturity_Year", functions.year(functions.col("Bucket_End_Date")));

        df = df.withColumn("paper_bm_temp", functions.when(df.col("Portfolio").isNotNull().and
                (functions.length(df.col("Portfolio")).$greater(0)).and
                (functions.length((functions.regexp_extract(functions.col("Portfolio"), "(.*)_PH", 0))).plus(
                        functions.length(functions.regexp_extract(functions.col("Portfolio"), "(.*)_BM", 0))).$greater(0)), "Yes"));


        df = df.withColumn("ph_temp", functions.when(df.col("Portfolio").isNotNull().and(df.col("Portfolio").endsWith("_PH")),"Yes"));
        df = df.withColumn("paper_bm_temp",
                functions.when(df.col("Portfolio").isNotNull(),
                        functions.when(df.col("Portfolio").endsWith("_PH").or(df.col("Portfolio").endsWith("_BM")),"Yes")
                ));

        df = df.withColumn("Exposure_Type", functions.when(functions.col("Portfolio").equalTo("FX"), functions.lit("FX")).otherwise("Curve"));


        df = df.withColumn("trade_status_temp", functions.when(functions.current_date().$less(functions.col("Bucket_End_Date")),
                "Active").otherwise(""));

        df = df.withColumn("fixed_float_temp",
                functions.when(functions.col("trade_status_temp").equalTo(functions.lit("Active")),
                        functions.lit("Float")).otherwise(""));


        df = df.withColumn("gamma_temp", functions.col("MR_Gamma_Base_UOM").multiply(functions.col("Conversion_Rate")));

        df = df.withColumn("vega_temp", functions.col("MR_Vega_Base_UOM").multiply(functions.col("Conversion_Rate")));

        df = df.withColumn("Curve_Delta", df.col("MR_Delta_Base_UOM").multiply(functions.col("Conversion_Rate")));

        df = df.withColumn("Delta", functions.col("Curve_Delta"));

        df = df.withColumn("Exposure", functions.when((functions.col("Curve_Delta").notEqual(functions.lit(0.0)).or
                (functions.col("gamma_temp").notEqual(functions.lit(0.0))).or
                (functions.col("vega_temp").notEqual(functions.lit(0.0)))), "Yes").otherwise(""));


        df = df.select(
                functions.lit("Endur").alias("Source"),
                df.col("Analysis_Date").alias("Valuation_Date"),
                functions.lit("Paper").alias("Cargo_Reference"),
                df.col("Portfolio"),
                df.col("Source_Curve_Name"),
                df.col("Curve_Name"),
                functions.lit("NON LNG TAC").alias("Trader_Curve_Type"),
                functions.lit("").alias("Curve_Type"),
                functions.lit("Endur").alias("Fobus_Report"),
                df.col("Fobus_Curve_Name"),
                df.col("limit_temp").alias("Limits_Group"),
                functions.lit("").alias("GridPoint"),
                df.col("Portfolio").alias("Book"),
                df.col("phys_fin_temp").alias("Physical_Financial"),
                df.col("phys_book_temp").alias("Physical_Book"),
                df.col("fin_book_temp").alias("Financial_Book"),
                df.col("ph_temp").alias("PH"),
                df.col("paper_bm_temp").alias("Paper_BM"),
                df.col("Internal_BU"),
                functions.lit("").alias("Counterparty"),
                df.col("Currency_ID").alias("Reporting_Currency"),
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
                df.col("Curve_Delta_UOM").alias("Curve_Delta_UOM"),
                functions.lit(0.0).alias("Price"),
                functions.lit("").alias("Price_UOM"),
                df.col("trade_status_temp").alias("Trade_Status"),
                df.col("fixed_float_temp").alias("Fixed_Float"),
                functions.lit("").alias("Delivery_Month"),
                functions.lit("").alias("Delivery_Year"),
                df.col("Delta"),
                df.col("MR_Gamma_Base_UOM").alias("Gamma"),
                df.col("MR_Vega_Base_UOM").alias("Vega"),
                df.col("Exposure"),
                functions.lit("").alias("Trade_Date"),
                functions.lit("").alias("Trade_ID"),
                functions.lit("").alias("Trade_Type"),
                functions.lit("Gas").alias("Commodity"),
                df.col("Run_Datetime"),
                df.col("Legal_Entity_Name"),
                functions.lit("").alias("Titan_Strategy"),
                functions.lit("Yes").alias("Liquid_Window")
        );
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/dl130_Endur_Monthly_Exposure_combined_format")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/dl130_market_risk_stage_result_summary_conformed") Dataset<Row> df,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions") Dataset<Row> df_ref_uom_conversions,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units") Dataset<Row> df_ref_conformed_units,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Curves") Dataset<Row> df_endur_curves,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Options_Curves") Dataset<Row> df_endur_option_curves

    ){
        return convert(df,
                df_endur_option_curves,
                df_endur_curves,
                df_ref_uom_conversions,
                df_ref_conformed_units
        );
    }

}
