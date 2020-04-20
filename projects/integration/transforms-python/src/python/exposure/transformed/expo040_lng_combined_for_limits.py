# ===== import: python function
import pyspark.sql.functions as f
import datetime
from pyspark.sql.types import IntegerType

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.exposure.schema_exposure.schema_expo040_combined_for_limits import combined_output_schema
from python.util.schema_utils import reorder_columns
from python.util.transforms_util import compute_bucket_season, compute_quarter, perform_rounding


def calc_no_of_wkdays(start, end):
    days = 0
    exclude = (6, 7)   # Exclude Saturday and Sunday
    while start <= end:
        if start.isoweekday() not in exclude:
            days += 1
        start += datetime.timedelta(days=1)
    return days


compute_no_of_week_days = f.udf(lambda st_dt, end_dt: calc_no_of_wkdays(st_dt, end_dt), IntegerType())

delta_cols = ['MR_Delta', 'MR_Delta_Conformed', 'MR_Delta_Therms', 'MR_Delta_MWh', 'MR_Delta_MMBtu'
              , 'MR_Delta_BBL', 'MR_Delta_EUR', 'MR_Delta_GBP']


def calc_date_join(df, df_ref, name, param):
    '''
    This function is used for calculating the result of all the delta fields. 

    Args:
        df (dataframe):
        df_ref (dataframe)
        name String: name column from Parameters
        param String: parameter_type column from Parameters

    Return:
        df: Trasnformed dataframe
    '''
    df = df.join(df_ref, (df['duplicateRow'] == df_ref['Name'])
                 & (df_ref['Name'] == f.lit(name))
                 & (df_ref['Parameter_Type'] == f.lit(param))
                 & (df_ref['Start_Date'] <= df['Valuation_Date'])
                 & ((df['Valuation_Date'] <= df_ref['End_Date']) | df_ref['End_Date'].isNull()),
                 how='left'
                )
    return df


def compute_delta_fields(df, *deltas):
    '''
    This function is used for calculating the result of all the delta fields. 

    Args:
        df (dataframe):
        deltas: list of all delta fields

    Return:
        df: Trasnformed dataframe
    '''
    df = df.withColumn('Value', f.when(df['Value'].isNotNull(), df['Value'])
                                 .otherwise(1))

    for delta in deltas:
        df = df.withColumn('MR_Delta', f.when(f.col('Bucket_End_Date') <= f.col('Valuation_Date'), 0)
                                        .when(((df['Bucket_Start_Date'] >= df['Valuation_Date']) &
                                               (df['Valuation_Date'] >= df['Bucket_End_Date'])),
                                              df[delta] * df['Value'] * (f.col('actual_no_days')/f.col('passive_no_days')))
                                        .when(f.col('Bucket_End_Date') > df['Valuation_Date'], df[delta] * df['Value'])
                                        .otherwise(df[delta]))
    return df


def compute_common_transform(df, val):
    '''
    This function is used for perform common transformation that applies to JCC and JLC
    Curve Names (Risk_Factor_Name)

    Args:
        df (dataframe):
        val (Value): lookup value from paramters ref data

    Return:
        df: Trasnformed dataframe
    '''
    df = df.withColumn('Bucket_Start_Date', f.add_months(df['Bucket_Start_Date'], val))
    df = df.withColumn('Bucket_End_Date', f.last_day(f.add_months(df['Bucket_Start_Date'], val)))
    df = df.withColumn('Bucket_Month', f.date_format(df['Bucket_End_Date'], 'MMM-yyyy'))

    df = df.withColumn('Valuation_Date', f.to_date(df['Valuation_Date']))
    df = df.withColumn('Valuation_Date_Start', f.trunc(df['Valuation_Date'], 'month'))
    df = df.withColumn('Valuation_Date_End', f.last_day(df['Valuation_Date']))

    df = compute_quarter(df, 'Bucket_Quarter', 'Bucket_End_Date')
    df = compute_bucket_season(df)
    df = df.withColumn('Bucket_Year', f.date_format(df['Bucket_End_Date'], 'yyyy'))

    df = df.withColumn("actual_no_days",
                       compute_no_of_week_days(f.col("Valuation_Date"), f.col("Valuation_Date_End")))
    df = df.withColumn("passive_no_days",
                       compute_no_of_week_days(f.col("Valuation_Date_Start"), f.col("Valuation_Date_End")))

    df = compute_delta_fields(df, *delta_cols)

    return df


def process_nymex(df, df_ref, delta_cols):
    '''
    This function is used for perform transformation that applies to Nymex

    Args:
        df (dataframe): Main dataframe
        df_ref: Ref dataframe
        delta_cols: List of all deltas that are double

    Return:
        df: Trasnformed dataframe
    '''
    ref_data = df_ref.where(df_ref["Parameter_Type"] == "Nymex Hub Volumetric")

    additional_df = df.join(ref_data,
                    (
                        (df['Projection_Index'] == ref_data["Name"]) &
                        (ref_data['Start_Date'] <= df['Valuation_Date']) &
                        ((df['Valuation_Date'] <= ref_data['End_Date']) | ref_data['End_Date'].isNull())
                    )
                 , how='inner')

    # multiply value_columns by value
    for col in delta_cols:
        additional_df = additional_df.withColumn(col, additional_df[col] * additional_df["Value"])

    additional_df = additional_df.withColumn("Risk_Factor_Name", f.lit("Nymex Hub Volumetric"))
    # select relevant columns using reorder_columns
    additional_df = reorder_columns(additional_df, combined_output_schema)
    df = df.union(additional_df)

    return df


def processor(df, df_ref):
    '''
    This is the main function which is used for invoking jcc and nymex function

    Args:
        df (dataframe): Main dataframe
        df_ref: Ref dataframe

    Return:
        df: Trasnformed dataframe
    '''
    df = df.where((df["Limits_Group"].isNull()) | (df["Limits_Group"] != "n/a"))
    df = df.where(df["Liquid_Window"] != "Outside Liquid Window")

    # Filter out records that are not 'JCC' and JLC
    df_not_jcc_and_jlc = df.filter((df['Risk_Factor_Name'] != 'JCC') & (df['Risk_Factor_Name'] != 'JLC'))

    # Filter out records with Risk_Factor_Name of 'JCC' and duplicate the rows
    df_jcc = df.filter(df['Risk_Factor_Name'] == f.lit('JCC'))
    df_jcc = df_jcc.withColumn("duplicateRow", f.explode(f.array(f.lit('M1'), f.lit('M2'))))

    # Row 1 of Risk_Factor_Name with JCC value
    df_jcc_1 = calc_date_join(df_jcc, df_ref, 'M1', 'JCC Multipliers')
    df_jcc_1 = compute_common_transform(df_jcc_1, -1)

    # Row 2 of Risk_Factor_Name with JCC value
    df_jcc_2 = calc_date_join(df_jcc, df_ref, 'M2', 'JCC Multipliers')
    df_jcc_2 = compute_common_transform(df_jcc_2, -2)

    df_jcc_result = df_jcc_1.union(df_jcc_2)
    df_jcc_result = df_jcc_result.withColumn('Risk_Factor_Name', f.lit('JCC - BD'))

    # Filter out records with Risk_Factor_Name of 'JLC' and duplicate the rows
    df_jlc = df.filter(df['Risk_Factor_Name'] == f.lit('JLC'))
    df_jlc = df_jlc.withColumn("duplicateRow", f.explode(f.array(f.lit('M4'), f.lit('M5'))))

    # Row 1 of Risk_Factor_Name with JLC value
    df_jlc_1 = calc_date_join(df_jlc, df_ref, 'M4', 'JLC Multipliers')
    df_jlc_1 = compute_common_transform(df_jlc_1, -4)

    # Row df_jlc_2 of Risk_Factor_Name with JLC value
    df_jlc_2 = calc_date_join(df_jlc, df_ref, 'M5', 'JLC Multipliers')
    df_jlc_2 = compute_common_transform(df_jlc_2, -5)

    df_jlc_result = df_jlc_1.union(df_jlc_2)
    df_jlc_result = df_jlc_result.withColumn('Risk_Factor_Name', f.lit('JLC - BD'))

    df_result = df_jlc_result.union(df_jcc_result)
 
    df_not_jcc_and_jlc = reorder_columns(df_not_jcc_and_jlc, combined_output_schema)
    df_result = reorder_columns(df_result, combined_output_schema)
    df_result = df_result.union(df_not_jcc_and_jlc)

    df_result = process_nymex(df_result, df_ref, delta_cols)
    df_result = perform_rounding(df_result, combined_output_schema)

    return df_result


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/transformed/LNG_combined_for_limits"),
    df_input=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/EXPO020_LNG_Combined_Summary"),
    df_ref=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Parameters")
)
def my_compute_function(df_input, df_ref):
    df = processor(df_input, df_ref)
    df = reorder_columns(df, combined_output_schema)
    return df
