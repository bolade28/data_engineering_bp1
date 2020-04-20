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
public final class dl130_market_risk_stage_result_summary {

    // The class for an automatically registered Transform contains the compute
    // function and information about the input/output datasets.
    // Automatic registration requires "@Input" and "@Output" annotations.
    @Compute    
    public void myComputeFunction(@Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl130_market_risk_stage_result_summary/dl130_market_risk_stage_result_summary") FoundryInput input,
        @Output("/BP/IST-IG-DD/data/technical/exposure_java/typed/endur/dl130_market_risk_stage_result_summary") FoundryOutput output
    ) {
        ReadRange readRange = BP_ReadRangeUtils.getReadRange(input);
        Dataset<Row> df = input.asDataFrame().readForRange(readRange);
        df = df.select(
                functions.to_timestamp(df.col("Run_Datetime"), "yyyy-MM-dd HH:mm:ss").alias("Run_Datetime"),
                df.col("Request_ID").cast("int"),
                functions.to_date(df.col("Analysis_Date"), "yyyy-MM-dd").alias("Analysis_Date"),
                df.col("Request_Type"),
                df.col("Business_Unit"),
                df.col("Portfolio"),
                df.col("Internal_BU"),
                df.col("Currency_ID"),
                df.col("Instrument_Type"),
                df.col("Toolset"),
                df.col("External_Portfolio"),
                df.col("Projection_Index"),
                df.col("UOM"),
                df.col("Grid_Point_ID").cast("int"),
                df.col("Grid_Point_Name"),
                functions.to_date(df.col("Grid_Point_Start_Date"), "yyyy-MM-dd").alias("Grid_Point_Start_Date"),
                functions.to_date(df.col("Grid_Point_End_Date"), "yyyy-MM-dd").alias("Grid_Point_End_Date"),
                functions.regexp_replace(df.col("Delta"), ",", "").cast("double").alias("Delta"),
                functions.regexp_replace(df.col("Gamma"), ",", "").cast("double").alias("Gamma"),
                df.col("Vega").cast("double"),
                df.col("Theta").cast("double"),
                df.col("Risk_Factor_Name"),
                df.col("Risk_Factor_CCY"),
                df.col("Risk_Factor_UOM"),
                df.col("MR_Delta").cast("double"),
                df.col("MR_Gamma").cast("double"),
                df.col("MR_Vega").cast("double"),
                df.col("MR_Theta").cast("double"),
                df.col("Market_Value").cast("double"),
                df.col("Bucket_Name"),
                df.col("Bucket_Id").cast("int"),
                df.col("Bucket_Order").cast("int"),
                functions.to_date(df.col("Bucket_Start_Date"), "yyyy-MM-dd").alias("Bucket_Start_Date"),
                functions.to_date(df.col("Bucket_End_Date"), "yyyy-MM-dd").alias("Bucket_End_Date"),
                df.col("Extract_Type"),
                df.col("Type")
        );
        output.getDataFrameWriter(df).write(BP_WriteRangeUtils.getWriteRange(input));

    }
}
