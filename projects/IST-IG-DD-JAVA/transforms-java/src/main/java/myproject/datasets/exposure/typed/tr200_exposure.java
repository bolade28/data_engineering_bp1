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
public final class tr200_exposure {

    // The class for an automatically registered Transform contains the compute
    // function and information about the input/output datasets.
    // Automatic registration requires "@Input" and "@Output" annotations.
    @Compute    
    public void myComputeFunction(@Input("/BP/IST-IG-SS-Systems/data/raw/titan/tr200_exposure/tr200_exposure") FoundryInput input,
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/typed/titan/tr200_exposure") FoundryOutput output) 
    {
        ReadRange readRange = BP_ReadRangeUtils.getReadRange(input);
        Dataset<Row> df = input.asDataFrame().readForRange(readRange);
        df = df.select(
                df.col("Valuation_Definition_Name"),
                df.col("Current_Valuation_Run_ID").cast("int"),
                df.col("Previous_Valuation_Run_ID").cast("int"),
                functions.to_date(df.col("Current_COB_Date"), "yyyy-MM-dd").alias("Current_COB_Date"),
                functions.to_date(df.col("Previous_COB_Date"), "yyyy-MM-dd").alias("Previous_COB_Date"),
                df.col("Delta_Type"),
                df.col("Delta_Sub_Type"),
                df.col("Exposure_Type"),
                df.col("Reporting_Currency"),
                df.col("Legal_Entity"),
                df.col("Counterparty"),
                df.col("Book"),
                df.col("Strategy"),
                df.col("Cargo_Reference"),
                df.col("Chain"),
                df.col("Product_Type"),
                df.col("Charge_Type"),
                df.col("Product"),
                df.col("PNL_Start_Year"),
                df.col("Book_Transfer").cast("boolean"),
                df.col("Trade_Group_ID").cast("int"),
                df.col("Trade_ID").cast("int"),
                df.col("Natural_Key"),
                df.col("Valuation_Key"),
                df.col("Valuation_Status"),
                df.col("Trade_Status"),
                df.col("Leg_Type"),
                functions.to_date(df.col("Settlement_Period_Start"), "yyyy-MM-dd").alias("Settlement_Period_Start"),
                functions.to_date(df.col("Settlement_Period_End"), "yyyy-MM-dd").alias("Settlement_Period_End"),
                df.col("Trade_Settlement_Period_ID").cast("int"),
                df.col("Delivery_Location"),
                functions.to_date(df.col("Trade_Date"), "yyyy-MM-dd").alias("Trade_Date"),
                df.col("Trader_NTID"),
                functions.to_date(df.col("Delivery_Start_Date"), "yyyy-MM-dd").alias("Delivery_Start_Date"),
                functions.to_date(df.col("Delivery_End_Date"), "yyyy-MM-dd").alias("Delivery_End_Date"),
                df.col("Buy_Sell"),
                df.col("Fixed_Float"),
                df.col("Trade_Currency"),
                df.col("Delta_Unit_Type"),
                df.col("Reporting_Tag"),
                df.col("Commodity_Group"),
                df.col("Reporting_Group"),
                df.col("Index_Name"),
                df.col("Curve_Name"),
                functions.to_date(df.col("Pricing_Maturity_Start"), "yyyy-MM-dd").alias("Pricing_Maturity_Start"),
                functions.to_date(df.col("Pricing_Maturity_End"), "yyyy-MM-dd").alias("Pricing_Maturity_End"),
                df.col("Pricing_Maturity_Period"),
                df.col("Point_Month"),
                functions.to_date(df.col("Point_Date"), "yyyy-MM-dd").alias("Point_Date"),
                df.col("Fixed_Float_Point"),
                df.col("Forward_Point").cast("int"),
                df.col("Historic_Point").cast("int"),
                df.col("Settled_Point").cast("int"),
                df.col("Price_UOM"),
                df.col("Price").cast("double"),
                df.col("Curve_Delta_UOM"),
                df.col("Curve_Delta").cast("double"),
                df.col("Index_Price_UOM"),
                df.col("Index_Price_Commodity_UOM"),
                df.col("FX_Trade_Currency"),
                df.col("FX_Reporting_Currency"),
                df.col("FX_Rate_Spot").cast("double"),
                df.col("FX_Rate_Forward").cast("double"),
                df.col("DCF_Trade_Currency").cast("double"),
                df.col("DCF_Report_Currency").cast("double"),
                df.col("Year_Fraction").cast("double"),
                df.col("Payment_Month"),
                functions.to_date(df.col("Payment_Date"), "yyyy-MM-dd").alias("Payment_Date"),
                df.col("FX_Fixed_Float"),
                df.col("FX_Delta").cast("double"),
                df.col("Grid_Point_Name"),
                df.col("CurveType"),
                df.col("TradeReference"),
                df.col("Curve_DCF_Report_Currency"),
                functions.to_timestamp(df.col("RunDateTime"), "yyyy-MM-dd HH:mm:ss").alias("RunDateTime")
        );
        output.getDataFrameWriter(df).write(BP_WriteRangeUtils.getWriteRange(input));
    }
}
