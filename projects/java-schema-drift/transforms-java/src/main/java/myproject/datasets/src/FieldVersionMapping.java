package myproject.datasets.src;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import org.apache.spark.sql.functions;


/**
* This is an example high-level Transform intended for automatic registration.
*/
public final class FieldVersionMapping {

  public static final int VERSION = 2;
  
  public static Seq<Column> mapping(int version) {
        Map<String, Integer> map = new LinkedHashMap<>();
        List<Column> lists = new ArrayList<>();

        map.put("Run_Datetime", 1);
        map.put("deal_num", 1);
        map.put("internal_portfolio", 1);
        map.put("param_seq_num", 1);
        map.put("param_seq_num_1", 1);
        map.put("Base_Currency", 1);
        map.put("Disc_Index", 1);
        map.put("Instrument_Type", 1);
        map.put("Portfolio", 1);
        map.put("Proj_Index", 1);
        map.put("Reval_Date", 1);
        map.put("Reval_Type", 1);
        map.put("Scenario_ID", 1);
        map.put("base_realized_value", 1);
        map.put("base_total_value", 1);
        map.put("base_unrealized_value", 1);
        map.put("broker_fee_type", 1);
        map.put("cflow_type", 1);
        map.put("change_in_tot_pnl", 1);
        map.put("comm_opt_exercised_flag", 1);
        map.put("currency_id", 1);
        map.put("df", 1);
        map.put("end_date", 1);
        map.put("event_source_id", 1);
        map.put("ins_num", 1);
        map.put("ins_seq_num", 1);
        map.put("ins_source_id", 1);
        map.put("new_deal", 1);
        map.put("price", 1);
        map.put("price_band", 1);
        map.put("price_band_seq_num", 1);
        map.put("profile_seq_num", 1);
        map.put("pymt", 1);
        map.put("pymt_date", 1);
        map.put("rate_dtmn_date", 1);
        map.put("rate_status", 1);
        map.put("realized_value", 1);
        map.put("settlement_type", 1);
        map.put("start_date", 1);
        map.put("strike", 1);
        map.put("total_value", 1);
        map.put("tran_num", 1);
        map.put("tran_status", 1);
        map.put("unrealized_value", 1);
        map.put("volume", 1);
        map.put("yest_base_realized_value", 1);
        map.put("yest_base_total_value", 1);
        map.put("yest_base_unrealized_value", 1);
        map.put("yest_pymt", 1);
        map.put("yest_realized_value", 1);
        map.put("yest_total_value", 1);
        map.put("yest_tran_status", 1);
        map.put("yest_unrealized_value", 1);
        map.put("Toolset", 2);
        map.put("Change_in_realised_value", 2); 
        map.put("Change_in_unrealised_value", 2); 
        map.put("Change_in_base_total_value", 2); 
        map.put("Change_in_base_unrealised_value", 2); 
        map.put("Change_in_base_realised_value", 2); 
        map.put("Internal_Business_Unit", 2);
        map.put("Internal_Legal_Entity", 2);
        map.put("External_Business_Unit", 2);
        map.put("External_Legal_Entity", 2);
        map.put("External_Portfolio", 2);
        map.put("Reference", 2);

        map.forEach((k, v) -> {
           if(v <= version) {
             lists.add(functions.col(k));
          }
        });
 
        return JavaConverters.asScalaBufferConverter(lists).asScala().toSeq();
  }

}
