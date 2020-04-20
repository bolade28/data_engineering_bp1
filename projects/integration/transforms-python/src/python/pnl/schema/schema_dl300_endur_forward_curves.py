from pyspark.sql.types import StructField, StructType, StringType, DateType, TimestampType, DoubleType, IntegerType

typed_output_schema = StructType([
        StructField('Run_Datetime', TimestampType(), True, {}),
        StructField('Valuation_Date', DateType(), True, {}),
        StructField('Source_Index_Name', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {}),
        StructField('Gridpoint_Date', DateType(), True, {}),
        StructField('Gridpoint_Name', StringType(), True, {}),
        StructField('Price', DoubleType(), True, {}),
        StructField('Price_Unit', StringType(), True, {}),
        StructField('Base_Currency', StringType(), True, {}),
        StructField('Bucket_Month', StringType(), True, {}),
        StructField('Bucket_Quarter', StringType(), True, {}),
        StructField('Bucket_Season', StringType(), True, {}),
        StructField('Bucket_Year', IntegerType(), True, {}),
])


transformed_output_schema = StructType([
        StructField('Run_Datetime', TimestampType(), True, {}),
        StructField('Valuation_Date', DateType(), True, {}),
        StructField('Source_Index_Name', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {}),
        StructField('Gridpoint_Date', DateType(), True, {}),
        StructField('Gridpoint_Name', StringType(), True, {}),
        StructField('Price', DoubleType(), True, {}),
        StructField('Price_Unit', StringType(), True, {}),
        StructField('Base_Currency', StringType(), True, {}),
        StructField('Bucket_Month', StringType(), True, {}),
        StructField('Bucket_Quarter', StringType(), True, {}),
        StructField('Bucket_Season', StringType(), True, {}),
        StructField('Bucket_Year', IntegerType(), True, {}),

])
