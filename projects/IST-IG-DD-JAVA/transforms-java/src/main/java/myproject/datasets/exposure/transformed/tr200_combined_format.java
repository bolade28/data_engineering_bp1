package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Date;

public class tr200_combined_format {

    public Dataset<Row> convert(Dataset<Row> df) {
        df = df.select(
                df.col("Source"),
                df.col("Valuation_Date"),
                df.col("Cargo_Reference"),
                df.col("Portfolio"),
                df.col("Source_Curve_Name"),
                df.col("Curve_Name"),
                df.col("Trader_Curve_Type"),
                df.col("Curve_Type"),
                df.col("Fobus_Report"),
                df.col("Fobus_Curve_Name"),
                df.col("Limits_Group"),
                df.col("GridPoint"),
                df.col("Book"),
                df.col("Physical_Financial"),
                df.col("Physical_Book"),
                df.col("Financial_Book"),
                df.col("PH"),
                df.col("Paper_BM"),
                df.col("Internal_BU"),
                df.col("Counterparty"),
                df.col("Reporting_Currency"),
                df.col("Product_Type"),
                df.col("Delta_Type"),
                df.col("Delta_Unit_Type"),
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
                df.col("Price"),
                df.col("Price_UOM"),
                df.col("Trade_Status"),
                df.col("Fixed_Float"),
                df.col("Delivery_Month"),
                df.col("Delivery_Year"),
                df.col("Delta"),
                df.col("Gamma"),
                df.col("Vega"),
                df.col("Exposure"),
                df.col("Trade_Date"),
                df.col("Trade_ID"),
                df.col("Trade_Type"),
                df.col("Commodity"),
                df.col("Run_Datetime"),
                df.col("Legal_Entity_Name"),
                df.col("Titan_Strategy")
        );
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/tr200_combined_format")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/tr200_exposure_conformed") Dataset<Row> df
    ){
        return convert(df);
    }
}
