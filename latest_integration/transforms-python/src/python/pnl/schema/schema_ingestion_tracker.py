from pyspark.sql.types import StructField, StructType, StringType, DateType

ingestion_schema = StructType([
    StructField('file_name', StringType(), False),
    StructField('Run_Datetime', StringType(), False),
    StructField('Valuation_Date', DateType(), False),
    StructField('epoch_process_time', StringType(), False),
    StructField('file_size', StringType(), False),
    StructField('modified', StringType(), False)
  ])
