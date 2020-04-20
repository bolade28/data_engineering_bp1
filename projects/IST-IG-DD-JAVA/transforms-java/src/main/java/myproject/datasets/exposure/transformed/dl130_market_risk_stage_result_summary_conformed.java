package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import myproject.datasets.util.BP_DatasetUtils;

import myproject.datasets.schema.*;
import java.util.Date;

public class dl130_market_risk_stage_result_summary_conformed {
//     Dataset<Row> convert(Dataset<Row> df,
//                          Dataset<Row> df_endur_counterparties
//     ) {
//         /////// conformed
//         df = df.withColumnRenamed("MR_Delta", "MR_Delta_Base_UOM")
//                 .withColumnRenamed("MR_Gamma", "MR_Gamma_Base_UOM")
//                 .withColumnRenamed("MR_Vega", "MR_Vega_Base_UOM")
//                 .withColumnRenamed("MR_Theta", "MR_Theta_Base_UOM")
//         ;
//         df_endur_counterparties = functions.broadcast(df_endur_counterparties.select(
//                 df_endur_counterparties.col("Party_Short_Name"),
//                 df_endur_counterparties.col("Party_Long_Name").alias("Legal_Entity_Name")
//         ));

//         df = df.join(df_endur_counterparties, df.col("Internal_BU").equalTo(df_endur_counterparties.col("Party_Short_Name")),"left")
//                 .drop("Party_Short_Name");

//         // temporary code
//         df = df.withColumn("Legal_Entity_Name", functions.lit("Dummy LE"));
//         df = df.withColumn("UOM",
//                 functions.when(df.col("UOM").equalTo("Currency"),
//                         functions.when(df.col("Risk_Factor_Name").equalTo("FX_EUR"), "EUR")
//                                 .when(df.col("Risk_Factor_Name").equalTo("FX_GBP"), "GBP")
//                                 .otherwise(df.col("UOM"))
//                 ).otherwise(df.col("UOM")));
//         return df;
//     }


   public Dataset<Row> convert(Dataset<Row> df, Dataset<Row> df_uom_conversions, Dataset<Row> df_endur_counterparties) {
        
        df_uom_conversions = df_uom_conversions.groupBy(df_uom_conversions.col("From_Unit")).pivot(df_uom_conversions.col("To_Unit")).sum("Conversion");

        df_uom_conversions = df_uom_conversions.select(df_uom_conversions.col("From_Unit"),
               functions.coalesce(df_uom_conversions.col("BBL"),functions.lit(0)).alias("BBL"),
               functions.coalesce(df_uom_conversions.col("MWh"),functions.lit(0)).alias("MWh"),
               functions.coalesce(df_uom_conversions.col("MMBtu"),functions.lit(0)).alias("MMBtu"),
               functions.coalesce(df_uom_conversions.col("Therms"),functions.lit(0)).alias("Therms"));

        df = df.withColumn("uom_gen",
                functions.when(df.col("UOM").equalTo(functions.lit("Currency")).and(df.col("Risk_Factor_Name").equalTo(functions.lit("FX_EUR"))), functions.lit("EUR"))
                        .when(df.col("UOM").equalTo(functions.lit("Currency")).and((df.col("Risk_Factor_Name").equalTo("FX_GBP"))), functions.lit("GBP"))
                        .otherwise(df.col("UOM")));
        
        df = df.withColumn("MR_Delta_EUR", functions.when(df.col("UOM").equalTo(functions.lit("Currency")).and
                   (df.col("Risk_Factor_Name").equalTo(functions.lit("FX_EUR"))), df.col("MR_Delta")).otherwise(functions.lit(0.0)));

        df = df.withColumn("MR_Delta_GBP", functions.when(df.col("UOM").equalTo(functions.lit("Currency")).and
                (df.col("Risk_Factor_Name").equalTo(functions.lit("FX_GBP"))), df.col("MR_Delta")).otherwise(functions.lit(0.0)));

        df_uom_conversions = functions.broadcast(df_uom_conversions);
        df = df.join(df_uom_conversions, df_uom_conversions.col("From_Unit").equalTo(df.col("UOM")), "left");

        df_endur_counterparties = functions.broadcast(df_endur_counterparties.select(
                df_endur_counterparties.col("Party_Short_Name"),
                df_endur_counterparties.col("Party_Long_Name").alias("Legal_Entity_Name")
        ));

        df = df.join(df_endur_counterparties, df.col("Internal_BU").equalTo(df_endur_counterparties.col("Party_Short_Name")),"left")
                .drop("Party_Short_Name");

        df = df.withColumn("MR_Delta_Therms", df.col("MR_Delta").multiply(df.col("Therms")))
                .withColumn("MR_Delta_MWh", df.col("MR_Delta").multiply(df.col("MWh")))
                .withColumn("MR_Delta_MMBtu", df.col("MR_Delta").multiply(df.col("MMBtu")))
                .withColumn("MR_Delta_BBL", df.col("MR_Delta").multiply(df.col("BBL")));
        
        df = df.drop("Party_Short_Name").drop("From_Unit2").drop(
                "UOM").drop("Therms").drop("MWh").drop("MMBtu").drop("BBL");

        df = df.withColumnRenamed("MR_Delta", "MR_Delta_Base_UOM")
                .withColumnRenamed("MR_Gamma", "MR_Gamma_Base_UOM")
                .withColumnRenamed("MR_Vega", "MR_Vega_Base_UOM")
                .withColumnRenamed("uom_gen", "UOM");

        // df = df.dropDuplicates();

        df = df.select(BP_DatasetUtils.getColumns(schema_dl130_market_risk_stage_result_summary_conformed.getSchema()));
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/dl130_market_risk_stage_result_summary_conformed")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/dl130_market_risk_stage_result_summary_monthly") Dataset<Row> df,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions") Dataset<Row> df_endur_conversions,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Counterparties") Dataset<Row> df_endur_counterparties
    ){
        return convert(df, df_endur_conversions, df_endur_counterparties);
    }
}
