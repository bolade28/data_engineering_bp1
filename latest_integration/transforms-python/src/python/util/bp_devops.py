# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions

# ===== import: our functions


def identify_processed_dataset(df, ref_data, dataset_name, valuation_date_col):
    """Transformation stage function
    This function:
        Version: T1
        Reads the raw and typed dataset along with the dataset name
        then write into a new data set where there is a join on 
        Valuation_date match and raw rundatetime greater than typed rundate time

    Args:
        params1 (dataframe): Raw dataset
        params2 (dataframe): typed dataset
        params3 (String)   : Name of the dataset to go in the notification DS for identification
        param4  (datetime) : Name of the Valuation Date ex: Reval_date

    Returns:
        dataframe: dataset
    """

    df = df.select(f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
                   f.to_date(df[valuation_date_col], 'yyyy-MM-dd').alias("Valuation_Date_raw"))
    df = df.groupby(df['Run_Datetime'], df['Valuation_Date_raw']).agg(
        f.count(f.lit(1)).alias("Row_Count"))

    df = df.select(df['Run_Datetime'], df['Valuation_Date_raw'], df['Row_Count'])
    ref_data = ref_data.groupby(ref_data['Run_Datetime'], ref_data[valuation_date_col]).agg(
        f.count(f.lit(1)).alias("Row_Count"))

    ref_data = ref_data.select(ref_data['Run_Datetime'].alias("Run_Datetime_typed"),
                               ref_data[valuation_date_col].alias("Valuation_Date_typed"),
                               ref_data['Row_Count'].alias("Row_Count_typed"))

    #  join to identify new file valuation date is already processed or not
    df = df.join(ref_data, ((df['Valuation_Date_raw'] == ref_data['Valuation_Date_typed']) &
                            (df['Run_Datetime'] > ref_data['Run_Datetime_typed'])), 'left')

    df = df.withColumn("Notification", f.when(df['Valuation_Date_typed'].isNotNull(),
                       f.lit("Old data set has received\n"))
                       .otherwise(f.lit(""))) \
        .withColumn("Current_Date", f.current_date()) \
        .withColumn("Dataset_Name", f.lit(dataset_name))

    df = df.filter(df['Run_Datetime_typed'].isNotNull())
    return df
