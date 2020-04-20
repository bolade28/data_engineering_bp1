package myproject.datasets.exposure.typed;


import com.palantir.transforms.lang.java.api.*;

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
public final class fp_power_m01015 {

    // The class for an automatically registered Transform contains the compute
    // function and information about the input/output datasets.
    // Automatic registration requires "@Input" and "@Output" annotations.
    @Compute      
    public void myComputeFunction(@Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_power_m01015/fp_power_m01015") FoundryInput input,
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/typed/freeport/fp_power_m01015") FoundryOutput output
    ) {
        ReadRange readRange = BP_ReadRangeUtils.getReadRange(input);
        Dataset<Row> df = input.asDataFrame().readForRange(readRange);

        df = df.na().drop("all", new String[] {"Valuation_Price"});
        df = df.select(
                functions.to_date(df.col("PLEX_Calculation_Date"), "yyyy/MM/dd").alias("PLEX_Calculation_Date"),
                df.col("Team_Name"),
                df.col("Book_Name"),
                df.col("Counterparty_Name"),
                df.col("Curve_Short_Name"),
                df.col("Notional_Qty_-_Current").cast("double").alias("Notional_Qty_Current"),
                functions.to_date(df.col("Trade_Date"), "yyyy/MM/dd").alias("Trade_Date"),
                df.col("Trade_ID").cast("long").alias("Trade_ID"),
                df.col("Commodity_Name"),
                df.col("Trade_Type_Code"),
                functions.to_date(df.col("Delivery_From_Period_Summary")).alias("Delivery_From_Period_Summary"),
                df.col("Valuation_Price").cast("double"),
                df.col("MTM_Price_-_Current").cast("double").alias("MTM_Price_Current"),
                df.col("MTM_Amt_-_Current").cast("double").alias("MTM_Amt_Current"),
                df.col("Disc_MTM_Amt_-_Current").cast("double").alias("Disc_MTM_Amt_Current"),
                df.col("Hedge_Qty_-_Current").cast("double").alias("Hedge_Qty_Current"),
                functions.to_timestamp(df.col("Run_Datetime"), "yyyy-MM-dd HH:mm:ss").alias("Run_Datetime")
        );
        output.getDataFrameWriter(df).write(BP_WriteRangeUtils.getWriteRange(input));
    }
}
