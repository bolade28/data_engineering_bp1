package myproject.datasets.schema;

import org.apache.spark.sql.types.*;

/**
* Instability of the environment prevented me from continuing to use reflection
* that is already implemented in BP_DatasetUtils
*/
public final class schema_dl130_market_risk_stage_result_summary_conformed {

    public static StructType getMonthlySchema() {
        return new StructType (new StructField[]{
                new StructField("Run_Datetime", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Request_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Analysis_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Request_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Business_Unit", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Portfolio", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Internal_BU", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Currency_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Instrument_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Toolset", DataTypes.StringType, true, Metadata.empty()),
                new StructField("External_Portfolio", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Projection_Index", DataTypes.StringType, true, Metadata.empty()),
                new StructField("UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Risk_Factor_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Risk_Factor_CCY", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Risk_Factor_UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MR_Delta", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Gamma", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Vega", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Theta", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Market_Value", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Bucket_Start_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Bucket_End_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Extract_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Bucket_Month", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Num_Days", DataTypes.LongType, true, Metadata.empty())
        });
    }


    public static StructType getSchema() {
        return new StructType(new StructField[]{
                new StructField("Run_Datetime", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Request_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Analysis_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Request_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Business_Unit", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Portfolio", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Internal_BU", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Currency_ID", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Instrument_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Toolset", DataTypes.StringType, true, Metadata.empty()),
                new StructField("External_Portfolio", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Projection_Index", DataTypes.StringType, true, Metadata.empty()),
                new StructField("UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Risk_Factor_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Risk_Factor_CCY", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Risk_Factor_UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MR_Delta_Base_UOM", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Gamma_Base_UOM", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Vega_Base_UOM", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Theta", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Market_Value", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Bucket_Start_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Bucket_End_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Extract_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Bucket_Month", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Legal_Entity_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MR_Delta_Therms", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Delta_MWh", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Delta_MMBtu", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Delta_BBL", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Delta_EUR", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MR_Delta_GBP", DataTypes.DoubleType, true, Metadata.empty())
        });

    }



    public static StructType getCombinedSchema() {
        return new StructType(new StructField[]{
                new StructField("Source", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Valuation_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Cargo_Reference", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Portfolio", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Source_Curve_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Curve_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Trader_Curve_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Curve_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Fobus_Report", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Fobus_Curve_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Limits_Group", DataTypes.StringType, true, Metadata.empty()),
                new StructField("GridPoint", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Book", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Physical_Financial", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Physical_Book", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Financial_Book", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PH", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Paper_BM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Internal_BU", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Counterparty", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Reporting_Currency", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Product_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Delta_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Delta_Unit_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Exposure_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Pricing_Maturity_Start", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Pricing_Maturity_End", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Pricing_Maturity_Period", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Pricing_Maturity_QTR", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Pricing_Maturity_Year", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Conversion_Rate", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unconformed_Curve_Delta", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unconformed_Curve_Delta_UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Curve_Delta", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Curve_Delta_UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Price_UOM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Trade_Status", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Fixed_Float", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Delivery_Month", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Delivery_Year", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Delta", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Gamma", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Vega", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Exposure", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Trade_Date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Trade_ID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Trade_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Commodity", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Run_Datetime", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("Legal_Entity_Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Titan_Strategy", DataTypes.StringType, true, Metadata.empty())
        });

    }
    
}
