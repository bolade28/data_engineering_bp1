package myproject.datasets.src;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
* This is an example high-level Transform intended for automatic registration.
*/
public final class ComputedType {

    public static Dataset<Row> process_df(Dataset<Row> df)
    {
        df = df.select(
        functions.to_timestamp(df.col("Run_Datetime"), "yyyy-MM-dd HH:mm:ss").alias("Run_Datetime"),
        df.col("deal_num").cast("int"),
        df.col("internal_portfolio").cast("int"),
        df.col("param_seq_num").cast("int"),
        df.col("param_seq_num_1").cast("int"),
        df.col("Base_Currency"),
        df.col("Disc_Index").cast("int"),
        df.col("Instrument_Type"),
        df.col("Portfolio"),
        df.col("Proj_Index").cast("int"),
        functions.to_date(df.col("Reval_Date"), "yyyy-MM-dd").alias("Reval_Date"),
        df.col("Reval_Type"),
        df.col("Scenario_ID").cast("int"),
        df.col("base_realized_value").cast("double"),
        df.col("base_total_value").cast("double"),
        df.col("base_unrealized_value").cast("double"),
        df.col("broker_fee_type").cast("int"),
        df.col("cflow_type"),
        df.col("change_in_tot_pnl").cast("double"),
        df.col("comm_opt_exercised_flag").cast("int"),
        df.col("currency_id"),
        df.col("df").cast("double"),
        functions.to_date(df.col("end_date"), "yyyy-MM-dd").alias("end_date"),
        df.col("event_source_id"),
        df.col("ins_num").cast("int"),
        df.col("ins_seq_num").cast("int"),
        df.col("ins_source_id").cast("int"),
        df.col("new_deal").cast("int"),
        df.col("price").cast("double"),
        df.col("price_band").cast("int"),
        df.col("price_band_seq_num").cast("int"),
        df.col("profile_seq_num").cast("int"),
        df.col("pymt").cast("double"),
        functions.to_date(df.col("pymt_date"), "yyyy-MM-dd").alias("pymt_date"),
        functions.to_date(df.col("rate_dtmn_date"), "yyyy-MM-dd").alias("rate_dtmn_date"),
        df.col("rate_status").cast("int"),
        df.col("realized_value").cast("double"),
        df.col("settlement_type"),
        functions.to_date(df.col("start_date"), "yyyy-MM-dd").alias("start_date"),
        df.col("strike").cast("double"),
        df.col("total_value").cast("double"),
        df.col("tran_num").cast("int"),
        df.col("tran_status"),
        df.col("unrealized_value").cast("double"),
        df.col("volume").cast("double"),
        df.col("yest_base_realized_value").cast("double"),
        df.col("yest_base_total_value").cast("double"),
        df.col("yest_base_unrealized_value").cast("double"),
        df.col("yest_pymt").cast("double"),
        df.col("yest_realized_value").cast("double"),
        df.col("yest_total_value").cast("double"),
        df.col("yest_tran_status").cast("int"),
        df.col("yest_unrealized_value").cast("double"),
        df.col("Toolset"),
        df.col("Change_in_realised_value").cast("double"), 
        df.col("Change_in_unrealised_value").cast("double"), 
        df.col("Change_in_base_total_value").cast("double"), 
        df.col("Change_in_base_unrealised_value").cast("double"), 
        df.col("Change_in_base_realised_value").cast("double"), 
        df.col("Internal_Business_Unit"),
        df.col("Internal_Legal_Entity"),
        df.col("External_Business_Unit"),
        df.col("External_Legal_Entity"),
        df.col("External_Portfolio"),
        df.col("Reference")
        );
        return df;    
    }

}
