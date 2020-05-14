from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, TimestampType

typed_output_schema = StructType([
    StructField('Run_Datetime', TimestampType(), True),
    StructField('Reval_Date', DateType(), True),
    StructField('Portfolio', StringType(), True),
    StructField('Deal_Number', IntegerType(), True),
    StructField('Adj_desc', StringType(), True),
    StructField('Effective_Date', DateType(), True),
    StructField('End_Date', DateType(), True),
    StructField('Pfolio_CCY', StringType(), True),
    StructField('Daily_Adj_Value_Pfolio_CCY', DoubleType(), True),
    StructField('Monthly_Adj_Value_Pfolio_CCY', DoubleType(), True),
    StructField('Yearly_Adj_Value_Pfolio_CCY', DoubleType(), True),
    StructField('Adj_CCY', StringType(), True),
    StructField('Daily_Adj_Value_Adj_CCY', DoubleType(), True),
    StructField('Monthly_Adj_Value_Adj_CCY', DoubleType(), True),
    StructField('Yearly_Adj_Value_Adj_CCY', DoubleType(), True),
])

transformed_output_schema = StructType([
    StructField('Valuation_Date', DateType(), True),
    StructField('Portfolio', StringType(), True),
    StructField('Deal_Number', IntegerType(), True),
    StructField('Adj_desc', StringType(), True),
    StructField('Effective_Date', DateType(), True),
    StructField('End_Date', DateType(), True),
    StructField('Pf_CCY', StringType(), True),
    StructField('DTD_Disc_Total_Adj_Pf', DoubleType(), True),
    StructField('MTD_Disc_Total_Adj_Pf', DoubleType(), True),
    StructField('YTD_Disc_Total_Adj_Pf', DoubleType(), True),
    StructField('Adj_CCY', StringType(), True),
    StructField('DTD_Disc_Total_Adj', DoubleType(), True),
    StructField('MTD_Disc_Total_Adj', DoubleType(), True),
    StructField('YTD_Disc_Total_Adj', DoubleType(), True),
    StructField('Run_Datetime', TimestampType(), True),
])
