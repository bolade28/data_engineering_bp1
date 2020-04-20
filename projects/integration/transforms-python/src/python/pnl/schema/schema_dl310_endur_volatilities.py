from pyspark.sql.types import StructField, StructType, StringType, DateType, TimestampType, DoubleType

typed_output_schema = StructType([
        StructField('Run_Datetime', TimestampType(), True, {}),
        StructField('Valuation_Date', DateType(), True, {}),
        StructField('Source_Index_Name', StringType(), True, {}),
        StructField('Volatility_Name', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {}),
        StructField('Gridpoint_Date', DateType(), True, {}),
        StructField('Gridpoint_Name', StringType(), True, {}),
        StructField('Layer', DoubleType(), True, {}),
        StructField('Pillar', DoubleType(), True, {}),
        StructField('Volatility', DoubleType(), True, {}),
])


transformed_output_schema = StructType([
        StructField('Run_Datetime', TimestampType(), True, {}),
        StructField('Valuation_Date', DateType(), True, {}),
        StructField('Source_Index_Name', StringType(), True, {}),
        StructField('Volatility_Name', StringType(), True, {}),
        StructField('Risk_Factor_Name', StringType(), True, {}),
        StructField('Gridpoint_Date', DateType(), True, {}),
        StructField('Gridpoint_Name', StringType(), True, {}),
        StructField('Pillar', DoubleType(), True, {}),
        StructField('Volatility', DoubleType(), True, {}),

])
