package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import java.util.Arrays;
import java.util.Date;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import myproject.datasets.util.BP_DatasetUtils;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import scala.collection.JavaConversions;
import static java.util.Arrays.asList;


/**
 * This is an example high-level Transform intended for automatic registration.
 */
public final class LNG_combined_exposure
{

    Dataset<Row>select_output_cols(Dataset<Row> df)
    {
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
                df.col("Pricing_Maturity_Start"),
                df.col("Pricing_Maturity_End"),
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

    public Dataset<Row> convert(
            Dataset<Row> df_tr200,
            Dataset<Row> df_endur,
            Dataset<Row> df_fp_power,
            Dataset<Row> df_fp_gas
    ) {
        df_fp_power = select_output_cols(df_fp_power);
        df_fp_gas = select_output_cols(df_fp_gas);
        df_endur = select_output_cols(df_endur);
        df_tr200 = select_output_cols(df_tr200);

        Dataset<Row> df = df_tr200;
        df = df.union(df_fp_power);
        df = df.union(df_fp_gas);
        df = df.union(df_endur);
        df = df.filter(df.col("Fobus_Curve_Name").notEqual("n/a"));
        return df;

    }
    // The class for an automatically registered Transform contains the compute
    // function and information about the input/output datasets.
    // Automatic registration requires "@Input" and "@Output" annotations.
    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/LNG_combined_exposure")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/tr200_combined_format") Dataset<Row> df_tr200,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/dl130_Endur_Monthly_Exposure_combined_format") Dataset<Row> df_endur,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_power_m01015_combined_format") Dataset<Row> df_fp_power,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_gas_m0145_combined_format") Dataset<Row> df_fp_gas


    )
    {
        return convert(df_tr200,
                df_endur,
                df_fp_power,
                df_fp_gas
        );
    }
}
