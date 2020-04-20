package myproject.datasets.exposure.typed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import com.palantir.transforms.lang.java.api.FoundryOutput;
import com.palantir.transforms.lang.java.api.FoundryInput;
import com.palantir.transforms.lang.java.api.ReadRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import myproject.datasets.util.BP_ReadRangeUtils;
import myproject.datasets.util.BP_WriteRangeUtils;

/**
 * This is an example high-level Transform intended for automatic registration.
 */
public final class fp_gas_m0145 {

    // The class for an automatically registered Transform contains the compute
    // function and information about the input/output datasets.
    // Automatic registration requires "@Input" and "@Output" annotations.
    @Compute    
    public void myComputeFunction(@Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_gas_m0145/fp_gas_m0145") FoundryInput input,
      @Output("/BP/IST-IG-DD/data/technical/exposure_java/typed/freeport/fp_gas_m0145") FoundryOutput output
    ) {
        ReadRange readRange = BP_ReadRangeUtils.getReadRange(input);
        Dataset<Row> df = input.asDataFrame().readForRange(readRange);
        df = df.select(
                df.col("Request_ID").cast("int"),
                functions.to_date(df.col("Analysis_Date"), "dd/MM/yyyy").alias("Analysis_Date"),
                df.col("Request_Type"),
                df.col("Contract_Month_DMD").cast("int"),
                df.col("Deal_Tracking_Num").cast("int"),
                df.col("Business_Unit"),
                df.col("Trader"),
                df.col("Portfolio"),
                df.col("External_BU"),
                df.col("Internal_BU"),
                df.col("Currency"),
                df.col("Instrument_Type"),
                functions.to_date(df.col("Trade_Date"), "dd/MM/yyyy").alias("Trade_Date"),
                functions.to_date(df.col("Trade_Time"), "dd/MM/yyyy").alias("Trade_Time"),
                df.col("Toolset"),
                df.col("External_Portfolio"),
                df.col("Broker"),
                df.col("OTC_Clearing_Broker").cast("int"),
                df.col("Offset_Transaction_Number").cast("int"),
                df.col("Reference"),
                df.col("Projection_Index"),
                df.col("UOM"),
                df.col("Grid_Point_ID").cast("int"),
                df.col("Grid_Point_Name"),
                functions.to_date(df.col("Grid_Point_Start_Date"), "dd/MM/yyyy").alias("Grid_Point_Start_Date"),
                functions.to_date(df.col("Grid_Point_End_Date"), "dd/MM/yyyy").alias("Grid_Point_End_Date"),
                functions.to_date(df.col("Grid_Point_Start_Date_Long"), "dd/MM/yyyy").alias("Grid_Point_Start_Date_Long"),
                functions.to_date(df.col("Grid_Point_End_Date_Long"), "dd/MM/yyyy").alias("Grid_Point_End_Date_Long"),
                df.col("Delta").cast("double"),
                df.col("Gamma").cast("double"),
                df.col("Vega").cast("double"),
                df.col("Theta").cast("double"),
                df.col("Risk_Factor_Name"),
                df.col("Delta_Shift").cast("double"),
                df.col("Gamma_Factor").cast("int"),
                df.col("Risk_Factor_CCY"),
                df.col("CCY_Conversion_Rate").cast("double"),
                df.col("Risk_Factor_UOM"),
                df.col("UOM_Conversion_Rate").cast("double"),
                df.col("MR_Delta").cast("double"),
                df.col("MR_Gamma").cast("double"),
                df.col("MR_Vega").cast("double"),
                df.col("MR_Theta").cast("double"),
                df.col("Market_Value").cast("double"),
                df.col("Bucket_Name"),
                df.col("Bucket_Id").cast("int"),
                df.col("Bucket_Order").cast("int"),
                functions.to_date(df.col("Bucket_Start_Date"), "dd/MM/yyyy").alias("Bucket_Start_Date"),
                functions.to_date(df.col("Bucket_End_Date"), "dd/MM/yyyy").alias("Bucket_End_Date"),
                df.col("Extract_Type"),
                df.col("Type"),
                functions.to_timestamp(df.col("Run_Datetime"), "yyyy-MM-dd HH:mm:ss").alias("Run_Datetime")
                );
        output.getDataFrameWriter(df).write(BP_WriteRangeUtils.getWriteRange(input));
    }
}
