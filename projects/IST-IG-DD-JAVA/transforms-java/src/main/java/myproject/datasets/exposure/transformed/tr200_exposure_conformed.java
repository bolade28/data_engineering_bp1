package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Date;
import scala.collection.JavaConversions;
import static java.util.Arrays.asList;

public class tr200_exposure_conformed {

    public Dataset<Row> convert(Dataset<Row> df,
                         Dataset<Row> df_ref_titan_curve,
                         Dataset<Row> df_ref_titan_conv,
                         Dataset<Row> df_ref_uom_conversions,
                         Dataset<Row> df_ref_conformed_units
    ) {

        df_ref_titan_curve = functions.broadcast(df_ref_titan_curve.withColumnRenamed("Curve_Name", "Titan_Curve_Name"));

        df_ref_titan_conv = functions.broadcast(df_ref_titan_conv
                .withColumnRenamed("Conversion", "Titan_Oil_Conversion_Rate")
                .withColumnRenamed("Curve_Name", "Titan_Oil_Curve_Name")
        );

        df_ref_uom_conversions = functions.broadcast(df_ref_uom_conversions.select(
                df_ref_uom_conversions.col("From_Unit").alias("Uom_From_Unit"),
                df_ref_uom_conversions.col("To_Unit").alias("Uom_To_Unit"),
                df_ref_uom_conversions.col("Conversion").alias("Uom_Conversion_Rate")
        ));

        df_ref_conformed_units = functions.broadcast(df_ref_conformed_units);


        df = df.join(df_ref_titan_curve, df.col("Curve_Name").equalTo(df_ref_titan_curve.col("Source_Curve_Name")), "left");
        df = df.join(df_ref_titan_conv, df.col("Titan_Curve_Name").equalTo(df_ref_titan_conv.col("Titan_Oil_Curve_Name")), "left");

        df = df.join(df_ref_conformed_units.filter(df_ref_conformed_units.col("BU").equalTo("LNG")), df.col("Curve_Delta_UOM").equalTo(df_ref_conformed_units.col("From_Unit")), "left");
        df = df.join(df_ref_uom_conversions, df.col("Curve_Delta_UOM").equalTo(df_ref_uom_conversions.col("Uom_From_Unit")).and(df.col("Conformed_Unit").equalTo(df_ref_uom_conversions.col("Uom_To_Unit"))), "left");


        df = df.withColumn("Trader_Curve_Type", functions.when(df.col("Titan_Curve_Name").contains("TAC"), "LNG TAC").otherwise("NON LNG TAC"));


        df = df.withColumn("Conformed_Curve_Delta_UOM",
                functions.when(df.col("Curve_Delta_UOM").isin("day", "Percentage", "EUR", "GBP", "USD"), df.col("Curve_Delta_UOM"))
                        .when(df.col("Curve_Name").equalTo("JLC"), "BBL")
                        .when(df.col("Curve_Delta_UOM").isin("MT", "Tonnes").and(df.col("Titan_Oil_Conversion_Rate").isNotNull()), "BBL")
                        .when(df.col("Curve_Delta_UOM").isin("MT", "Tonnes").and(df.col("Titan_Oil_Conversion_Rate").isNull()), df.col("Curve_Delta_UOM"))
                        .when(df.col("Uom_Conversion_Rate").isNotNull(), df.col("Conformed_Unit"))
                        .otherwise(df.col("Curve_Delta_UOM"))
        );

        df = df.withColumn("Conformed_Conversion_Rate",
                functions.when(df.col("Curve_Delta_UOM").isin("day", "Percentage", "EUR", "GBP", "USD"), 1.0)
                        .when(df.col("Curve_Name").equalTo("JLC"), 1.0)
                        .when(df.col("Curve_Delta_UOM").isin("MT", "Tonnes").and(df.col("Titan_Oil_Conversion_Rate").isNotNull()), df.col("Titan_Oil_Conversion_Rate"))
                        .when(df.col("Curve_Delta_UOM").isin("MT", "Tonnes").and(df.col("Titan_Oil_Conversion_Rate").isNull()), 1.0)
                        .when(df.col("Uom_Conversion_Rate").isNotNull(), df.col("Uom_Conversion_Rate"))
                        .otherwise(functions.lit(1.0))
        );

        df = df.withColumn("Conformed_Curve_Delta", df.col("Curve_Delta")
                .multiply(df.col("Conformed_Conversion_Rate")))
                .withColumn("Pricing_Maturity_QTR", functions.concat(
                        functions.date_format(df.col("Pricing_Maturity_End"), "yyyy"),
                        functions.lit(" Q"),
                        functions.quarter(df.col("Pricing_Maturity_End"))
                ))
                .withColumn("Pricing_Maturity_Year", functions.date_format(df.col("Pricing_Maturity_End"), "yyyy"));

        df = df.na().fill("Mapping not found", JavaConversions.asScalaBuffer(asList("Fobus_Curve_Name")));
 
        df = df.select(
                functions.lit("Titan").alias("Source"),
                df.col("Current_COB_Date").alias("Valuation_Date"),
                df.col("Cargo_Reference"),
                functions.lit("").alias("Portfolio"),
                df.col("Curve_Name").alias("Source_Curve_Name"),
                df.col("Titan_Curve_Name").alias("Curve_Name"),  // fill nulls with "unmapped"
                df.col("Trader_Curve_Type"),
                df.col("CurveType").alias("Curve_Type"),
                functions.lit("Titan").alias("Fobus_Report"),
                df.col("Fobus_Curve_Name"),
                df.col("Limits_Group"),
                df.col("Grid_Point_Name").alias("GridPoint"),
                df.col("Book"),
                functions.lit("Physical").alias("Physical_Financial"),
                df.col("Book").alias("Physical_Book"),
                functions.lit("").alias("Financial_Book"),
                functions.lit("Yes").alias("PH"),
                functions.lit("").alias("Paper_BM"),
                functions.lit("").alias("Internal_BU"),
                df.col("Counterparty"),
                df.col("Reporting_Currency"),
                df.col("Product_Type"),
                df.col("Delta_Type"),
                df.col("Delta_Unit_Type"),
                df.col("Exposure_Type"),
                df.col("Pricing_Maturity_Start"),
                df.col("Pricing_Maturity_End"),
                df.col("Pricing_Maturity_Period"),
                df.col("Pricing_Maturity_QTR"),
                df.col("Pricing_Maturity_Year"),
                df.col("Conformed_Conversion_Rate").alias("Conversion_Rate"),
                df.col("Curve_Delta").alias("Unconformed_Curve_Delta"),
                df.col("Curve_Delta_UOM").alias("Unconformed_Curve_Delta_UOM"),
                df.col("Conformed_Curve_Delta").alias("Curve_Delta"),
                df.col("Conformed_Curve_Delta_UOM").alias("Curve_Delta_UOM"),
                df.col("Price"),
                df.col("Price_UOM"),
                df.col("Trade_Status"),
                df.col("Fixed_Float"),
                functions.date_format(df.col("Delivery_End_Date"), "MM").alias("Delivery_Month"),
                functions.date_format(df.col("Delivery_End_Date"), "yyyy").alias("Delivery_Year"),
                df.col("Conformed_Curve_Delta").alias("Delta"),
                functions.lit(0.0).alias("Gamma"),
                functions.lit(0.0).alias("Vega"),
                functions.lit(0.0).alias("Exposure"),
                df.col("Trade_Date"),
                df.col("Trade_ID"),
                functions.lit("").alias("Trade_Type"),
                functions.lit("").alias("Commodity"),
                df.col("RunDateTime").alias("Run_Datetime"),
                df.col("Legal_Entity").alias("Legal_Entity_Name"),
                df.col("Strategy").alias("Titan_Strategy"),
                functions.lit("Yes").alias("Liquid_Window")
        );

        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/tr200_exposure_conformed")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/current/titan/tr200_exposure") Dataset<Row> df_titan_input,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Titan_Curves") Dataset<Row> df_ref_titan_curve,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Titan_Oil_UOM_Conversions") Dataset<Row> df_ref_titan_conv,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions") Dataset<Row> df_ref_uom_conversions,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units") Dataset<Row> df_ref_conformed_units
    ) {
        Dataset<Row> df_titan_output = convert(
                df_titan_input,
                df_ref_titan_curve,
                df_ref_titan_conv,
                df_ref_uom_conversions,
                df_ref_conformed_units);
        return df_titan_output;
    }
}
