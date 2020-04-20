# ===== import: python function
import pyspark.sql.functions as f
from uuid import uuid4

# ===== import: palantir functions

# ===== import: our functions
from python.util.bp_constants import VALUATION_DATE_COL

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def mmbtu_therms_lookup(df_ref_conv):
    '''
    This function will be used for selectively rounding double fields so that it appears
    as integer to the user.
    '''
    output = {}
    rows = df_ref_conv.select('Conversion', 'To_Unit').filter(
        ((df_ref_conv['From_Unit'] == f.lit('MWh')) & (df_ref_conv['To_Unit'] == f.lit('Therms'))) |
        ((df_ref_conv['From_Unit'] == f.lit('MWh')) & (df_ref_conv['To_Unit'] == f.lit('MMBtu')))
    ).distinct().take(2)

    for row in rows:
        output[row['To_Unit']] = row['Conversion']
    return output


def compute_quarter(df, target_name, source_name):
    '''
    This function will be used for computing quarter in format 2019 Q3

    Args:
        df (Dataframe):
        target_name: column to be cre

    Returns:
        Trasnformed dataframe
    '''
    df = df.withColumn(target_name,
                       f.concat(f.year(f.col(source_name)).cast("string"),
                                f.lit(" Q"), f.quarter(f.col(source_name))))
    return df


def compute_typed(df, schema):
    '''
    This function will be used for computing the first stage (Typed) LNG data pipeline.
    It is also
     used for casting column to the appropriate data type.
    Args:
        df (Dataframe):
        schema:

    Returns:
        Trasnformed dataframe
    '''
    MAPCF = {"IntegerType": "int",
             "DoubleType": "double",
             "FloatType": "float"}

    df_out = df
    logger.info("======= DEVELOPER compute_typed STARTED. ========")
    for row in schema:
        literals = row.metadata.get("default")
        if literals:
            df_out = df_out.withColumn(row.name, f.lit(literals))
        elif str(row.dataType) == "DateType":
            iformat = row.metadata.get("dt_format") if row.metadata.get("dt_format") else DEFAULT_DATE_FORMAT
            df_out = df_out.withColumn(row.name, f.to_date(df[row.name], iformat).alias(row.name))
        elif str(row.dataType) == "TimestampType":
            iformat = row.metadata.get("tst_format") if row.metadata.get("tst_format") else DEFAULT_TIMESTAMP_FORMAT
            df_out = df_out.withColumn(row.name, f.to_timestamp(df[row.name], iformat).alias(row.name))
        elif str(row.dataType) in ["IntegerType", "FloatType", "DoubleType"]:
            df_out = df_out.withColumn(row.name, df[row.name].cast(MAPCF[str(row.dataType)]))

    logger.info("======== DEVELOPER compute_typed FINISHED ======")
    return df_out


def reorder_columns(df, schema):
    '''
    This function will be used for reordering columns position in the output dataframe.

    Args:
        df (Dataframe):
        schema:

    Returns:
        Trasnformed dataframe
    '''
    expected_fields = list(map(lambda x: x.name, schema))
    return df.select(*expected_fields)


def compute_constants_and_rename(df, schema):
    '''
    This function will be used for setting constant values and rename fields where necessary.

    Args:
        df (Dataframe):
        schema:

    Returns:
        Trasnformed dataframe
    '''
    df_out = df
    for row in schema:
        old = row.metadata.get("from")
        constant = row.metadata.get("default")
        if old:
            df_out = df_out.withColumnRenamed(old, row.name)
        elif constant == 0 or constant == 1:
            df_out = df_out.withColumn(row.name, f.lit(constant))
        elif constant == "None":
            df_out = df_out.withColumn(row.name, f.lit(None).cast("string"))
        elif constant:
            df_out = df_out.withColumn(row.name, f.lit(constant))
        else:
            df_out = df_out

    return df_out


def copy_field_value(df, schema):
    '''
    This function will be used for setting copying fields where necessary.

    Args:
        df (Dataframe):
        schema:

    Returns:
        Trasnformed dataframe
    '''
    df_out = df
    for row in schema:
        copy = row.metadata.get("copy")
        if copy:
            df_out = df_out.withColumn(row.name, df_out[copy])
        else:
            df_out = df_out

    return df_out


def compute_bucket_season(df):
    '''
    This function will be used for computing bucket season

    Args:
        df (Dataframe):

    Returns:
        Trasnformed dataframe
    '''
    df = df.withColumn('Bucket_Season',
            f.concat(
                (f.date_format(f.add_months(df['Bucket_End_Date'], -3), 'yyyy ')),
                f.when(
                    f.month(f.add_months(df['Bucket_End_Date'], -3)) > 6,
                    f.lit("W")
                )
                .otherwise(
                    f.lit("S")
                )
            )
    )
    return df


def compute_liquid_window(df, src_start_date, src_analys_date, target):
    '''
    This function will be used for computing Liquid Window for a given dataset.

    Args:
        df (Dataframe):
        src_start_date:

    Returns:
        Trasnformed dataframe
    '''
    df = df.withColumn(target, f.when(f.col(src_start_date) < f.date_add(f.add_months(f.last_day(f.col(src_analys_date)), 36), 1)
                                                , "Liquid Window").otherwise(f.lit('Outside Liquid Window')))
    return df


def perform_rounding(df, schema):
    '''
    This function will be used for selectively rounding double fields so that it appears
    as integer to the user.
    '''

    df_out = df
    for row in schema:
        rounding = row.metadata.get("rounding")
        if rounding:
            if str(row.dataType) == "DoubleType":
                df_out = df_out.withColumn(row.name, f.ceil(f.round(f.col(row.name), 0)))
        else:
            df_out = df_out

    return df_out


def custom_join(df, df_ref, flag, data_col, ref_col, *lookup_vals, **rename):
    '''
    This function is for performing join between your dataframe(data) and dataframe(ref),
    The flag variable is required for renaming.

    Args:
        flag (String): String to append to your data fields so they can be renamed to avoid ambiguity.a
        data_col(String): Column from the dataframe associated with your data.
        ref_col(String): Column from reference dataframe associated with your refence data.
        lookup_vals(Arrays): Arrays of fields that you want to lookup.
        rename(dictionary): Dictionary that you can use for renaming some of your fields.

    Returns:
        Dataframe after performing transformation.

    Example:
        df = single_custom_join(df, ref_data_endur_conterparties,
                     '_0', 'External_Business_Unit', 'Party_Short_Name', 
                     'Party_Long_Name', 'Start_Date', 'End_Date')
        _0 -> flag, 'External_Business_Unit' -> col from your main_df,
        'Party_Short_Name' -> col from your ref dataframe and
        'Party_Long_Name', 'Start_Date', 'End_Date' are your lookups value.
    '''

    df_ref = f.broadcast(df_ref.select(f.col(ref_col).alias(ref_col + flag),
                         *[f.col(lookup_val).alias(lookup_val + flag) for lookup_val in lookup_vals]))

    df = df.join(df_ref, (df[data_col] == df_ref[ref_col + flag])
                 & (df_ref['Start_Date' + flag] <= df['Valuation_Date'])
                 & ((df['Valuation_Date'] >= df_ref['End_Date' + flag]) | (df_ref['End_Date' + flag].isNull())),
                 how='left')

    df = df.withColumn('Data_Status', f.when(
                                            (df['Start_Date' + flag].isNull()), #failed join
                                            f.lit('ERROR')
                                        ).otherwise(
                                            f.lit('OK')
                                        )
                        )

    for key, value in rename.items():
        df = df.withColumnRenamed(key, value)

    return df


def error_checkers(df, ref_dataset, *attributes):
    '''
    This function is used for checking errors associated with each lookup.

    Example:
    df = error_checkers(df, Bench', 'Strategy', 'Team', 'Trader')
    '''
    if attributes:
        for attribute in attributes:
            df = df.withColumn("{}Null".format(attribute), f.when(df[attribute].isNull(),
                               f.lit("Look up of {} on {} failed.\n".format(ref_dataset, attribute)))
                               .otherwise(f.lit("")))
    return df


def calc_data_status_desc(df, arrays):
    '''
    This function is used for checking errors associated with each lookup.

    Returns:
        Dataframe after performing transformation.

    Example:
    df = calc_data_status_desc(df, ['BenchNull', 'StrategyNull', 'TeamNull', 'TraderNull'])
    '''
    df = df.withColumn("Data_Status_Description", f.concat(*arrays))
    return df


def overwrite_field_content(df, date_logic, *attributes):
    '''
    This function is used for checking errors associated with each lookup.

    Returns:
        Dataframe after performing transformation.

    Example:
        df = overwrite_field_content(df, Date_Logic_Pass, 'Bench', 'Strategy', 'Team', 'Trader')
    '''

    for attribute in attributes:
        df = df.withColumn(attribute, f.when(df[date_logic] == "PASS", df[attribute]))
    return df


def cal_each_ref_join_data_status(data_df, start_date_alias, data_status_alias):
    data_df = data_df.withColumn('Data_Status_' + data_status_alias, f.when((data_df[start_date_alias].isNull()),
                                 f.lit('ERROR')).otherwise(f.lit('OK')))
    return data_df


def cal_final_data_status(data_df):
    '''
    This function is used for compting the value of Data_Status field.

    Returns:
        Dataframe after performing transformation.

    '''
    data_df = data_df.withColumn('Data_Status', f.when(f.instr(data_df['Data_Status_Description'],
                                                       'failed') > 0, 'ERROR').otherwise('OK'))

    return data_df


def ref_data_join(df,
                  df_ref,
                  data_source,
                  ref_source,
                  start_date_col='Start_Date',
                  end_date_col='End_Date',
                  valuation_date_col=VALUATION_DATE_COL,
                  lookup_columns=None):
    '''
    This function is used for reference data lookups.

    Args:
        df(DataFrame): DataFrame containing the column on which the lookup is done.
        df_ref(DataFrame): DataFrame containing the reference data (must include start and end date columns).
        data_source(String): Column in df containing the values to be looked up.
        ref_source(String): Column in df_ref in which the lookup is performed.
        start_date_col(String): Column in df_ref giving the validity start date of a row (None = unbounded)
        end_date_col(String): Column in df_ref giving the validity end date of a row (None = unbounded)
        lookup_columns(dictionary): Columns to retrieve from df_ref based on the lookup.
                            Maps from df_ref's columns to the return value DataFrame's column names.
                            Any columns from df_ref not mentioned here are not returned.

    Returns:
        Dataframe after performing transformation.

    See unit tests in test_transformed_util.py for examples of use.
    '''
    suffix = '_' + str(uuid4())
    cols_to_exclude = set([ref_source, start_date_col, end_date_col])
    lookup_vals = lookup_columns.keys() - cols_to_exclude

    df_ref = f.broadcast(
        df_ref.select(*[f.col(c).alias(c + suffix) for c in lookup_vals | cols_to_exclude]))

    df = df.join(
        df_ref,
        (df[data_source] == df_ref[ref_source + suffix])
        & ((df_ref[start_date_col + suffix] <= df[valuation_date_col]) | (df_ref[start_date_col + suffix].isNull()))
        & ((df[valuation_date_col] <= df_ref[end_date_col + suffix]) | (df_ref[end_date_col + suffix].isNull())),
        how='left')

    for key, value in lookup_columns.items():
        df = df.withColumnRenamed(key + suffix, value)

    cols_to_drop = [
        (col + suffix) for col in [ref_source, start_date_col, end_date_col] if col not in lookup_columns.keys()
    ]
    df = df.drop(*cols_to_drop)

    return df


def cal_RefData_Datetime(data_df):
    data_df = data_df.withColumn('RefData_Datetime',
                                 f.greatest(*[val for val in data_df.columns if 'Last_Updated_Timestamp' in val]))
    return data_df


def calc_bucket_logic(df):
    df = compute_quarter(df, 'Bucket_Year', 'Bucket_End_Date')
    df = compute_quarter(df, 'Bucket_Quarter', 'Bucket_End_Date')
    df = df.withColumn("Bucket_Month", f.date_format(df["Bucket_End_Date"], 'MMM-yyyy'))
    df = compute_bucket_season(df)
    return df
