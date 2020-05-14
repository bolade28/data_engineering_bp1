from pyspark.sql.types import StructField, StructType, DateType, TimestampType

raw_out_schema_reval = StructType([
        StructField('Run_Datetime', TimestampType(), True, {'tst_format': 'yyyy-MM-dd HH:mm:ss'}),
        StructField('Reval_Date', DateType(), True, {'dt_format': 'yyyy-MM-dd'}),
])

raw_out_schema_820 = StructType([
        StructField('Run_Datetime', TimestampType(), True, {'tst_format': 'yyyy-MM-dd HH:mm:ss'}),
        StructField('Reval_Date', DateType(), True, {'dt_format': 'dd-MMM-yyyy'}),
])

raw_out_schema_valuation = StructType([
        StructField('Run_Datetime', TimestampType(), True, {'tst_format': 'yyyy-MM-dd HH:mm:ss'}),
        StructField('Valuation_Date', DateType(), True, {}),
])


raw_out_schema_analysis = StructType([
        StructField('Run_Datetime', TimestampType(), True, {'tst_format': 'yyyy-MM-dd HH:mm:ss'}),
        StructField('Analysis_Date', DateType(), True, {}),
])
