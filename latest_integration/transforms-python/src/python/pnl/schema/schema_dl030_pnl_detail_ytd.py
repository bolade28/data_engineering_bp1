from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, TimestampType

typed_output_schema = StructType([
    StructField('Run_Datetime', TimestampType(), True),
    StructField('deal_num', IntegerType(), True),
    StructField('Base_Currency', StringType(), True),
    StructField('Disc_Index', IntegerType(), True),
    StructField('Instrument_Type', StringType(), True),
    StructField('Portfolio', StringType(), True),
    StructField('Proj_Index', IntegerType(), True),
    StructField('Reval_Date', DateType(), True),
    StructField('Reval_Type', StringType(), True),
    StructField('Scenario_ID', IntegerType(), True),
    StructField('base_realized_value', DoubleType(), True),
    StructField('base_total_value', DoubleType(), True),
    StructField('base_unrealized_value', DoubleType(), True),
    StructField('broker_fee_type', IntegerType(), True),
    StructField('cflow_type', StringType(), True),
    StructField('comm_opt_exercised_flag', IntegerType(), True),
    StructField('currency_id', StringType(), True),
    StructField('deal_leg', IntegerType(), True),
    StructField('deal_leg_1', IntegerType(), True),
    StructField('deal_pdc', IntegerType(), True),
    StructField('df', DoubleType(), True),
    StructField('end_date', DateType(), True),
    StructField('event_source_id', StringType(), True),
    StructField('ins_seq_num', IntegerType(), True),
    StructField('ins_source_id', IntegerType(), True),
    StructField('price', DoubleType(), True),
    StructField('price_band', IntegerType(), True),
    StructField('price_band_seq_num', IntegerType(), True),
    StructField('pymt', DoubleType(), True),
    StructField('pymt_date', DateType(), True),
    StructField('rate_dtmn_date', DateType(), True),
    StructField('rate_status', IntegerType(), True),
    StructField('realized_value', DoubleType(), True),
    StructField('settlement_type', StringType(), True),
    StructField('start_date', DateType(), True),
    StructField('strike', DoubleType(), True),
    StructField('total_value', DoubleType(), True),
    StructField('tran_num', IntegerType(), True),
    StructField('tran_status', StringType(), True),
    StructField('unrealized_value', DoubleType(), True),
    StructField('volume', DoubleType(), True),
])

transformed_output_schema = StructType([
    StructField('Valuation_Date', DateType(), True),
    StructField('Deal_Number', IntegerType(), True),
    StructField('Tran_Status', StringType(), True),
    StructField('Portfolio', StringType(), True),
    StructField('Instrument_Type', StringType(), True),
    StructField('Start_Date', DateType(), True),
    StructField('End_Date', DateType(), True),
    StructField('LTD_Real_Disc_Pf', DoubleType(), True),
    StructField('LTD_Unreal_Disc_Pf', DoubleType(), True),
    StructField('LTD_Total_PnL_Disc_Pf', DoubleType(), True),
    StructField('LTD_Real_Disc_Deal', DoubleType(), True),
    StructField('LTD_Unreal_Disc_Deal', DoubleType(), True),
    StructField('LTD_Total_PnL_Disc_Deal', DoubleType(), True),
    StructField('Pymt', DoubleType(), True),
    StructField('Run_Datetime', TimestampType(), True),
    StructField('Base_Currency', StringType(), True),
    StructField('Disc_Index', IntegerType(), True),
    StructField('Proj_Index', IntegerType(), True),
    StructField('Reval_Type', StringType(), True),
    StructField('Scenario_Id', IntegerType(), True),
    StructField('Broker_Fee_Type', IntegerType(), True),
    StructField('Cflow_Type', StringType(), True),
    StructField('Comm_Opt_Exercised_Flag', IntegerType(), True),
    StructField('Currency_Id', StringType(), True),
    StructField('Df', DoubleType(), True),
    StructField('Event_Source_Id', StringType(), True),
    StructField('Ins_Seq_Num', IntegerType(), True),
    StructField('Ins_Source_Id', IntegerType(), True),
    StructField('Price', DoubleType(), True),
    StructField('Price_Band', IntegerType(), True),
    StructField('Price_Band_Seq_Num', IntegerType(), True),
    StructField('Pymt_Date', DateType(), True),
    StructField('Rate_Dtmn_Date', DateType(), True),
    StructField('Rate_Status', IntegerType(), True),
    StructField('Settlement_Type', StringType(), True),
    StructField('Strike', DoubleType(), True),
    StructField('Tran_Num', IntegerType(), True),
    StructField('Volume', DoubleType(), True),
    StructField('Deal_Leg', IntegerType(), True),
    StructField('Deal_Leg_1', IntegerType(), True),
    StructField('Deal_Pdc', IntegerType(), True),
])
