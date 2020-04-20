package myproject.datasets.exposure.transformed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import com.palantir.transforms.lang.java.api.TransformContext;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.util.Date;
import java.util.Iterator;
import java.util.Vector;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import myproject.datasets.exposure.model.DL130_Model;
import myproject.datasets.exposure.model.DL130_Model_Mapped;
import myproject.datasets.util.BP_DatasetUtils;
import myproject.datasets.util.BP_DateUtils;
import myproject.datasets.util.BP_SchemaUtil;
import myproject.datasets.schema.schema_dl130_market_risk_stage_result_summary_conformed;
import java.util.*;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


public class dl130_market_risk_stage_result_summary_monthly {
    private static final Logger LOG = LoggerFactory.getLogger(dl130_market_risk_stage_result_summary_monthly.class);
    
    public static UDF2<Date, Date, Seq<String>> expudf = (Date d1, Date d2) -> BP_DateUtils.calc_monthly_buckets(d1, d2);

    public static UDF1<Date, String> bucket_date_month = (Date d1) -> new SimpleDateFormat("MMM-yyyy").format(d1);

    // static SimpleDateFormat MONTH_DATE_FORMAT = new SimpleDateFormat("MMM-yyyy");

    public Dataset<Row> convert(TransformContext context, Dataset<Row> df) {
        SparkSession sparkSession = context.sparkSession();
        
        LOG.info("Monthly bucket exec Started");
        Date[] dates = BP_DateUtils.getStartEndDate();

        df = BP_DateUtils.filterLatest(df, dates[0], dates[1], "Analysis_Date", "Run_Datetime");
        // LOG.info("Monthly bucket exec finished, Filtered. " + df.count() + " rows.");

        df = df.groupBy(df.col("Run_Datetime"),
                df.col("Request_ID"),
                df.col("Analysis_Date"),
                df.col("Request_Type"),
                df.col("Business_Unit"),
                df.col("Internal_BU"),
                df.col("Portfolio"),
                df.col("Currency_ID"),
                df.col("Instrument_Type"),
                df.col("Toolset"),
                df.col("External_Portfolio"),
                df.col("Projection_Index"),
                df.col("UOM"),
                df.col("Risk_Factor_Name"),
                df.col("Risk_Factor_CCY"),
                df.col("Risk_Factor_UOM"),
                df.col("Extract_Type"),
                df.col("Grid_Point_Start_Date"),
                df.col("Grid_Point_End_Date"),
                df.col("Type"))
                .agg(functions.sum(df.col("MR_Delta")).alias("MR_Delta"),
                        functions.sum(df.col("MR_Gamma")).alias("MR_Gamma"),
                        functions.sum(df.col("MR_Vega")).alias("MR_Vega"),
                        functions.sum(df.col("MR_Theta")).alias("MR_Theta"),
                        functions.sum(df.col("Market_Value")).alias("Market_Value"));
        
        sparkSession.udf().register("expudf", expudf, DataTypes.createArrayType(DataTypes.StringType));

        sparkSession.udf().register("bucket_date_month", bucket_date_month, DataTypes.StringType);

        // ===========================================
        df = df.withColumn("monthly_bucket_array", functions.explode(functions.callUDF("expudf",
                functions.to_date(df.col("Grid_Point_Start_Date"), "yyyy-MM-dd"),
                functions.to_date(df.col("Grid_Point_End_Date"), "yyyy-MM-dd"))).alias("fields2"));

        // Deseralzing MonthlyBucket class: (comp_start_date)#(comp_end_date)#(comp_num_days) -> comp_start_date, comp_end_date, comp_num_days
        df = df.withColumn("comp_start_date", functions.substring(df.col("monthly_bucket_array"), 1, 10))
                .withColumn("comp_end_date", functions.substring(df.col("monthly_bucket_array"), 12, 10))
                .withColumn("comp_num_days", functions.substring(df.col("monthly_bucket_array"), 23, 20));
        // =========================================== output: comp_start_date, comp_end_date, comp_num_days
        
        df = df.withColumn("Total_Num_Days", functions.datediff(df.col("Grid_Point_End_Date"), df.col("Grid_Point_Start_Date")).$plus(functions.lit(1)));
        df = df.withColumn("day_delta", functions.when(functions.col("Total_Num_Days").notEqual(0), functions.col("MR_Delta").$div(functions.col("Total_Num_Days"))).otherwise(0.));
        df = df.withColumn("day_gamma", functions.when(functions.col("Total_Num_Days").notEqual(0), functions.col("MR_Gamma").$div(functions.col("Total_Num_Days"))).otherwise(0.));
        df = df.withColumn("day_vega", functions.when(functions.col("Total_Num_Days").notEqual(0), functions.col("MR_Vega").$div(functions.col("Total_Num_Days"))).otherwise(0.));
        df = df.withColumn("day_theta", functions.when(functions.col("Total_Num_Days").notEqual(0), functions.col("MR_Theta").$div(functions.col("Total_Num_Days"))).otherwise(0.));
        df = df.withColumn("day_market_value", functions.when(functions.col("Total_Num_Days").notEqual(0), functions.col("Market_Value").$div(functions.col("Total_Num_Days"))).otherwise(0.));
        
        // ===========================================
        df = df.withColumn("MR_Delta", functions.round(functions.col("comp_num_days").multiply(functions.col("day_delta")), 6));
        df = df.withColumn("MR_Gamma", functions.round(functions.col("comp_num_days").multiply(functions.col("day_gamma")), 6));
        df = df.withColumn("MR_Vega", functions.round(functions.col("comp_num_days").multiply(functions.col("day_vega")), 6));
        df = df.withColumn("MR_Theta", functions.round(functions.col("comp_num_days").multiply(functions.col("day_theta")), 6));
        df = df.withColumn("Market_Value", functions.round(functions.col("comp_num_days").multiply(functions.col("day_market_value")), 6));
        df = df.withColumn("Comp_Num_Days", functions.col("comp_num_days"));

        // ===========================================
        df = df.withColumn("Bucket_Start_Date", functions.trunc(functions.col("comp_start_date"), "month"));
        df = df.withColumn("Bucket_End_Date", functions.last_day(functions.col("comp_end_date")));

        df = df.withColumn("Bucket_Month", functions.callUDF("bucket_date_month",
                df.col("Bucket_Start_Date")));

        df = df.withColumnRenamed("Comp_Num_Days", "Num_Days");

        // reordering
        org.apache.spark.sql.types.StructType schema = schema_dl130_market_risk_stage_result_summary_conformed.getMonthlySchema();
        df = df.select(BP_SchemaUtil.getColNamesOrdered(schema));
        
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/dl130_market_risk_stage_result_summary_monthly")
    public Dataset<Row> myComputeFunction(TransformContext context, 
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/current/endur/dl130_market_risk_stage_result_summary") Dataset<Row> df
    ){
        return convert(context, df);
    }



}
