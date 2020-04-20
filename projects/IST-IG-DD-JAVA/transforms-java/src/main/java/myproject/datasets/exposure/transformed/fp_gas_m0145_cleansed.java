package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Date;

public class fp_gas_m0145_cleansed {
    Dataset<Row> convert(Dataset<Row> df
            , Dataset<Row> df_nagp_counterparties
            , Dataset<Row> df_nagp_uoms

                         ) {
        df = df.withColumnRenamed("Analysis_Date", "Valuation_Date")
                .withColumn("Contract_Month_Date",
                        functions.to_date(df.col("Contract_Month_DMD").cast(DataTypes.StringType), "yyyyMM"))
                .withColumn("Trader", functions.lower(df.col("Trader")))
                .withColumnRenamed("Broker", "Orig_Broker")
                .withColumnRenamed("OTC_Clearing_Broker", "Orig_OTC_Clearing_Broker")
                .withColumnRenamed("UOM", "Orig_UOM")
                .withColumnRenamed("Business_Unit", "Orig_Business_Unit")
        ;

        df_nagp_counterparties = functions.broadcast(df_nagp_counterparties);

        Dataset<Row> df_map;

        // Business_Unit
        df_map = df_nagp_counterparties.select(
                df_nagp_counterparties.col("Short_Name"),
                df_nagp_counterparties.col("Legal_Entity_Name").alias("Business_Unit")
        );
        df = df.join(df_map, df.col("Orig_Business_Unit").equalTo(df_map.col("Short_Name")), "left").drop("Short_Name");

        //Broker
        df_map = df_nagp_counterparties.select(
                df_nagp_counterparties.col("Short_Name"),
                df_nagp_counterparties.col("Legal_Entity_Name").alias("Broker")
        );
        df = df.join(df_map, df.col("Orig_Broker").equalTo(df_map.col("Short_Name")), "left").drop("Short_Name");

        //OTC_Clearing_Broker
        df_map = df_nagp_counterparties.select(
                df_nagp_counterparties.col("Icos_ID"),
                df_nagp_counterparties.col("Legal_Entity_Name").alias("OTC_Clearing_Broker")
        );
        df = df.join(df_map, df.col("Orig_OTC_Clearing_Broker").equalTo(df_map.col("Icos_ID")), "left").drop("Icos_ID");

        //UOM
        df = df.join(df_nagp_uoms, df.col("Orig_UOM").equalTo(df_nagp_uoms.col("NAGP_Name")), "left").drop("NAGP_Name");
        df = df.withColumn("UOM", functions.coalesce(df.col("UoM_Name"), df.col("Orig_UOM")));

        //Legal_Entity_Name
        df_map = df_nagp_counterparties.select(
                df_nagp_counterparties.col("Short_Name"),
                df_nagp_counterparties.col("Legal_Entity_Name")
        );
        df = df.join(df_map, df.col("Internal_BU").equalTo(df_map.col("Short_Name")), "left").drop("Short_Name");

        //Counterparty
        df_map = df_nagp_counterparties.select(
                df_nagp_counterparties.col("Short_Name"),
                df_nagp_counterparties.col("Legal_Entity_Name").alias("Counterparty")
        );
        df = df.join(df_map, df.col("External_BU").equalTo(df_map.col("Short_Name")), "left").drop("Short_Name");

        df = df.select(
                df.col("Request_ID"),
                df.col("Valuation_Date"),
                df.col("Request_Type"),
                df.col("Contract_Month_Date"),
                df.col("Deal_Tracking_Num"),
                df.col("Business_Unit"),
                df.col("Trader"),
                df.col("Portfolio"),
                df.col("Counterparty"),
                df.col("Internal_BU"),
                df.col("Currency"),
                df.col("Instrument_Type"),
                df.col("Trade_Date"),
                df.col("Trade_Time"),
                df.col("Toolset"),
                df.col("External_Portfolio"),
                df.col("Broker"),
                df.col("OTC_Clearing_Broker"),
                df.col("Offset_Transaction_Number"),
                df.col("Reference"),
                df.col("Projection_Index"),
                df.col("UOM"),
                df.col("Grid_Point_ID"),
                df.col("Grid_Point_Name"),
                df.col("Grid_Point_Start_Date"),
                df.col("Grid_Point_End_Date"),
                df.col("Grid_Point_Start_Date_Long"),
                df.col("Grid_Point_End_Date_Long"),
                df.col("Delta"),
                df.col("Gamma"),
                df.col("Vega"),
                df.col("Theta"),
                df.col("Risk_Factor_Name"),
                df.col("Delta_Shift"),
                df.col("Gamma_Factor"),
                df.col("Risk_Factor_CCY"),
                df.col("CCY_Conversion_Rate"),
                df.col("Risk_Factor_UOM"),
                df.col("UOM_Conversion_Rate"),
                df.col("MR_Delta"),
                df.col("MR_Gamma"),
                df.col("MR_Vega"),
                df.col("MR_Theta"),
                df.col("Market_Value"),
                df.col("Bucket_Name"),
                df.col("Bucket_Id"),
                df.col("Bucket_Order"),
                df.col("Bucket_Start_Date"),
                df.col("Bucket_End_Date"),
                df.col("Extract_Type"),
                df.col("Type"),
                df.col("Run_Datetime"),
                df.col("Legal_Entity_Name")
        );
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_gas_m0145_cleansed")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/current/freeport/fp_gas_m0145") Dataset<Row> df,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Counterparties") Dataset<Row> df_ref_nagp_counterparties,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_UOMs") Dataset<Row> df_ref_nagp_uoms
    ){
        return convert(
                df
                , df_ref_nagp_counterparties
                , df_ref_nagp_uoms
        );
    }

}
