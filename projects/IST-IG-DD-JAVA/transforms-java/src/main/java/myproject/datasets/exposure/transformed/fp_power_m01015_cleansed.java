package myproject.datasets.exposure.transformed;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Date;

public class fp_power_m01015_cleansed {
    public Dataset<Row> convert(Dataset<Row> df) {
        df = df.withColumnRenamed("PLEX_Calculation_Date", "Valuation_Date")
                .withColumnRenamed("Team_Name", "Portfolio")
                .withColumnRenamed("Book_Name", "Book")
                .withColumnRenamed("Counterparty_Name", "Counterparty")
                .withColumnRenamed("Curve_Short_Name", "Curve_Name")
                .withColumnRenamed("Commodity_Name", "Commodity")
                .withColumnRenamed("Delivery_From_Period_Summary", "Delivery_From_Period")
                .withColumnRenamed("Valuation_Price", "Price")
                .withColumn("UoM",
                        functions.when(df.col("Trade_Type_Code").isNull(), "Unknown")
                                .when(df.col("Trade_Type_Code").equalTo("PFWD"), functions.lit("MWh"))
                                .when(df.col("Trade_Type_Code").equalTo("FX_EUR"), functions.lit("EUR"))
                                .when(df.col("Trade_Type_Code").equalTo("FX_GBP"), functions.lit("GBP"))
                )
                .withColumn("Currency", functions.lit("USD"));
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/fp_power_m01015_cleansed")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/current/freeport/fp_power_m01015") Dataset<Row> df
    ){
        return convert(df);
    }
}
