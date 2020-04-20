from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, TimestampType

typed_output_schema = StructType([
        StructField('Request_ID', IntegerType(), True, {}),
        StructField('Analysis_Date', DateType(), True, {}),
        StructField('Request_Type', StringType(), True, {}),
        StructField('Contract_Month_Date', DateType(), True, {}),
        StructField('Deal_Tracking_Num', IntegerType(), True, {}),
        StructField('Business_Unit', StringType(), True, {}),
        StructField('Trader', StringType(), True, {}),
        StructField('Portfolio', StringType(), True, {}),
        StructField('External_BU', StringType(), True, {}),
        StructField('Internal_BU', StringType(), True, {}),
        StructField('Instrument_Type', StringType(), True, {}),
        StructField('Trade_Date', DateType(), True, {}),
        StructField('Trade_Time', DateType(), True, {}),
        StructField('Toolset', StringType(), True, {}),
        StructField('External_Portfolio', StringType(), True, {}),
        StructField('Instrument_Type', StringType(), True, {}),
        StructField('Broker', StringType(), True, {}),
        StructField('OTC_Clearing_Broker', StringType(), True, {}),
        StructField('Offset_Transaction_Number', IntegerType(), True, {}),
        StructField('Reference', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {}),
        StructField('UOM', StringType(), True, {}),
        StructField('Grid_Point_ID', IntegerType(), True, {}),
        StructField('Grid_Point_ID', IntegerType(), True, {}),
        StructField('Grid_Point_Name', StringType(), True, {}),
        StructField('Grid_Point_Name', StringType(), True, {}),
        StructField('Grid_Point_Start_Date', DateType(), True, {}),
        StructField('Grid_Point_End_Date', DateType(), True, {}),
        StructField('Grid_Point_Start_Date_Long', DateType(), True, {}),
        StructField('Grid_Point_End_Date_Long', DateType(), True, {}),
        StructField('Gamma', DoubleType(), True, {}),
        StructField('Vega', DoubleType(), True, {}),
        StructField('Theta', DoubleType(), True, {}),
        StructField('Projection_Index', StringType(), True, {}),
        StructField('Delta_Shift', DoubleType(), True, {}),
        StructField('Gamma_Factor', IntegerType(), True, {}),
        StructField('Risk_Factor_CCY', StringType(), True, {}),
        StructField('CCY_Conversion_Rate', DoubleType(), True, {}),
        StructField('Risk_Factor_UOM', StringType(), True, {}),
        StructField('UOM_Conversion_Rate', DoubleType(), True, {}),
        StructField('MR_Delta', DoubleType(), True, {}),
        StructField('MR_Gamma', DoubleType(), True, {}),
        StructField('MR_Vega', DoubleType(), True, {}),
        StructField('MR_Theta', DoubleType(), True, {}),
        StructField('Market_Value', DoubleType(), True, {}),
        StructField('Bucket_Name', StringType(), True, {}),
        StructField('Bucket_Id', IntegerType(), True, {}),
        StructField('Bucket_Order', IntegerType(), True, {}),
        StructField('Bucket_Start_Date', DateType(), True, {}),
        StructField('Bucket_End_Date', DateType(), True, {}),
        StructField('Extract_Type', StringType(), True, {}),
        StructField('Type', StringType(), True, {}),
        StructField('Run_Datetime', StringType(), True, {}),
        StructField('Internal_BU', StringType(), True, {}),
])

transformed_output_schema = StructType([
        StructField('Source', StringType(), True, {'default': 'S3 Gas'}),
        StructField('Valuation_Date', DateType(), True, {'from': 'Analysis_Date'}),
        StructField('Portfolio/Book', StringType(), True, {'from': 'Portfolio'}),
        StructField('Projection_Index', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {'copy': 'Curve_Name'}),
        StructField('Limits_Group', StringType(), True, {}),
        StructField('MR_Delta', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_UOM', StringType(), True, {}),
        StructField('MR_Delta_Conformed', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_Conformed_UOM', StringType(), True, {}),
        StructField('MR_Delta_Therms', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_MWh', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_MMBtu', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_BBL', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_EUR', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_GBP', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Gamma', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Vega', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Theta', DoubleType(), True, {'rounding': '0'}),
        StructField('Market_Value', DoubleType(), True, {'rounding': '0'}),
        StructField('Conversion_Rate', DoubleType(), True, {}),
        StructField('Bucket_Start_Date', DateType(), True, {}),
        StructField('Bucket_End_Date', DateType(), True, {}),
        StructField('Bucket_Month', DateType(), True, {}),
        StructField('Bucket_Quarter', StringType(), True, {}),
        StructField('Bucket_Season', StringType(), True, {}),
        StructField('Bucket_Year', StringType(), True, {}),
        StructField('Bucket_Name', StringType(), True, {'from': 'Bucket_Name'}),
        StructField('Bucket_Id', IntegerType(), True, {'from': 'Bucket_Id'}),
        StructField('Bucket_Order', IntegerType(), True, {'from': 'Bucket_Order'}),
        StructField('Grid_Point_ID', IntegerType(), True, {}),
        StructField('Grid_Point_Name', StringType(), True, {}),
        StructField('Grid_Point_Start_Date', DateType(), True, {'from': 'Grid_Point_Start_Date_Long'}),
        StructField('Grid_Point_End_Date', DateType(), True, {'from': 'Grid_Point_End_Date_Long'}),
        StructField('Grid_Point_Month', DateType(), True, {}),
        StructField('Counterparty', StringType(), True, {}),
        StructField('Legal_Entity_Name', StringType(), True, {}),
        StructField('Internal_BU', StringType(), True, {}),
        StructField('External_BU', StringType(), True, {'default': "None"}),
        StructField('External_Portfolio', StringType(), True, {}),
        StructField('Price', DoubleType(), True, {'default': 0.}),
        StructField('Price_UOM', StringType(), True, {'default': "None"}),
        StructField('Trade_Status', StringType(), True, {}),
        StructField('Trade_Date', DateType(), True, {}),
        StructField('Deal_Number', IntegerType(), True, {'from': 'Deal_Tracking_Num'}),
        StructField('Deal_Trader', StringType(), True, {'from': 'Trader'}),
        StructField('Instrument_Type', StringType(), True, {}),
        StructField('Expo_Type', StringType(), True, {'from': 'Type'}),
        StructField('Product_Type', StringType(), True, {'copy': 'Instrument_Type'}),
        StructField('Delta_Unit_Type', StringType(), True, {'default': 'Curve Price Commodity UOM'}),
        StructField('Cargo_Delivery_Month', StringType(), True, {'default': "None"}),
        StructField('Cargo_Delivery_Year', StringType(), True, {'default': "None"}),
        StructField('Cargo_Delivery_Location', StringType(), True, {'default': "None"}),
        StructField('Cargo_Reference', StringType(), True, {'default': "None"}),
        StructField('Titan_Strategy', StringType(), True, {'default': "None"}),
        StructField('Liquid_Window', StringType(), True, {}),
        StructField('Bench', StringType(), True, {}),
        StructField('Team', StringType(), True, {}),
        StructField('Strategy', StringType(), True, {}),
        StructField('Trader', StringType(), True, {}),
        StructField('Data_Status', StringType(), True, {}),
        StructField('Data_Status_Description', StringType(), True, {}),
        StructField('Run_Datetime', TimestampType(), True, {}),
        StructField('RefData_Datetime', TimestampType(), True, {})
])


combined_output_schema = StructType([
        StructField('Source', StringType(), True, {}),
        StructField('Valuation_Date', DateType(), True, {}),
        StructField('Portfolio/Book', StringType(), True, {}),
        StructField('Projection_Index', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {}),
        StructField('Limits_Group', StringType(), True, {}),
        StructField('MR_Delta', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_UOM', StringType(), True, {}),
        StructField('MR_Delta_Conformed', DoubleType(), True, {}),
        StructField('MR_Delta_Conformed_UOM', StringType(), True, {}),
        StructField('MR_Delta_Therms', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_MWh', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_MMBtu', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_BBL', DoubleType(), True, {'rounding': '0'}),
        StructField('MR_Delta_EUR', DoubleType(), True, {}),
        StructField('MR_Delta_GBP', DoubleType(), True, {}),
        StructField('MR_Gamma', DoubleType(), True, {}),
        StructField('MR_Vega', DoubleType(), True, {}),
        StructField('MR_Theta', DoubleType(), True, {}),
        StructField('Market_Value', DoubleType(), True, {}),
        StructField('Conversion_Rate', DoubleType(), True, {}),
        StructField('Bucket_Start_Date', DateType(), True, {}),
        StructField('Bucket_End_Date', DateType(), True, {}),
        StructField('Bucket_Month', DateType(), True, {}),
        StructField('Bucket_Quarter', StringType(), True, {}),
        StructField('Bucket_Season', StringType(), True, {}),
        StructField('Bucket_Year', StringType(), True, {}),
        StructField('Bucket_Name', StringType(), True, {'from': 'Bucket_Name'}),
        StructField('Bucket_Id', IntegerType(), True, {'from': 'Bucket_Id'}),
        StructField('Bucket_Order', IntegerType(), True, {'from': 'Bucket_Order'}),
        StructField('Grid_Point_ID', IntegerType(), True, {}),
        StructField('Grid_Point_Name', StringType(), True, {}),
        StructField('Grid_Point_Start_Date', DateType(), True, {'from': 'Grid_Point_Start_Date_Long'}),
        StructField('Grid_Point_End_Date', DateType(), True, {'from': 'Grid_Point_End_Date_Long'}),
        StructField('Grid_Point_Month', DateType(), True, {}),
        StructField('Counterparty', StringType(), True, {}),
        StructField('Legal_Entity_Name', StringType(), True, {}),
        StructField('Internal_BU', StringType(), True, {}),
        StructField('External_Portfolio', StringType(), True, {}),
        StructField('Price', DoubleType(), True, {}),
        StructField('Price_UOM', StringType(), True, {}),
        StructField('Trade_Status', StringType(), True, {}),
        StructField('Trade_Date', DateType(), True, {}),
        StructField('Deal_Number', IntegerType(), True, {}),
        StructField('Instrument_Type', StringType(), True, {}),
        StructField('Expo_Type', StringType(), True, {}),
        StructField('Product_Type', StringType(), True, {}),
        StructField('Delta_Unit_Type', StringType(), True, {}),
        StructField('Cargo_Delivery_Month', StringType(), True, {}),
        StructField('Cargo_Delivery_Year', StringType(), True, {}),
        StructField('Cargo_Delivery_Location', StringType(), True, {}),
        StructField('Cargo_Reference', StringType(), True, {}),
        StructField('Titan_Strategy', StringType(), True, {}),
        StructField('Liquid_Window', StringType(), True, {}),
        StructField('Run_Datetime', TimestampType(), True, {}),
        StructField('RefData_Datetime', TimestampType(), True, {}),
])
