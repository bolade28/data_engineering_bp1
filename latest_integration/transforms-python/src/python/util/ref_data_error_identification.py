# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions

# ===== import: our functions
from python.util.bp_constants import VALUATION_DATE_COL


def get_ref_data_status_column(df, ref, ref_data_name, col_name_list, ref_portfolio_col_name, ref_last_updated_col_name,
                               start_date_col="Start_Date",
                               end_date_col="End_Date",
                               valuation_date_col=VALUATION_DATE_COL,
                               portfolio_col="Portfolio"
                               ):
    """ Transformation stage function
    This function:
        Takes df and ref_data dataframes, which have their columns renamed to avoid column name clashes
        Joins them together on logic:
            start_date_col < df.Valuation_Date < end_date_col
            Portfolio column(df.Portfolio == ref[ref_portfolio_col_name])
        Determines if there is missing data:
            If there is:
                Data status = ERROR
                Data status description = string which holds the details of the error
            If no:
                Data status = OK
                Data status description = null

    Args:
        params1 (dataframe): df
        params2 (dataframe): ref_data_name
        params3 (string): ref_data_name  = used for the data_status_description e.g. "Portfolio_Attribute"
        params4 (list): col_name_list = list of strings of the column names which are to be included from
                        the ref_data to the new dataframe
        params5 (string): ref_portfolio_col_name  = Portfolio renamned version in ref e.g. "Uom_Portfolio"
        params6 (string): ref_last_updated_col_name = Last_Updated_Timestamp renamed version in ref
                          e.g. "Uom_Last_Updated_Timestamp"
        params7 (string): column containing reference data start date, default="Start_Date"
        params8 (string): column containing reference data end date, default="End_Date
    Returns:
        dataframe: dataframe joined with ref_data containing data_status + data_status_description columns
    """
    # ===== Formating reference dataframe
    def_ref_data = ref.withColumnRenamed("Portfolio", ref_portfolio_col_name)
    df_ref_data = f.broadcast(def_ref_data)

    # === join df with ref_data
    df = df.join(
        df_ref_data,
        (f.upper(df[portfolio_col]) == f.upper(df_ref_data[ref_portfolio_col_name])) &
        (df_ref_data[start_date_col] <= df[valuation_date_col]) &
        ((df[valuation_date_col] <= df_ref_data[end_date_col]) | df_ref_data[end_date_col].isNull()),
        how='left')

    # ==== Determine Error or OK -> null rows (Last_Updated_Timestamp must have value if matched)
    df = df.withColumn(
        'Data_Status',
        f.when(
            df[ref_last_updated_col_name].isNull(),  # failed join
            f.lit('ERROR')
        ).otherwise(
            f.lit('OK')
        )
    )
    # ==== Description column -> adding error statement
    if 'Data_Status_Description' not in df.columns:
        df = df.withColumn('Data_Status_Description', f.lit(''))

    for col_name in col_name_list:
        col_name_error_statement = col_name + '_error_statement'
        print(col_name_error_statement)
        error_statement = 'Look up on ' + col_name + ' on ' + ref_data_name + ' failed\n'
        # Add error statment to a holder column
        df = df.withColumn(
            col_name_error_statement,
            f.when(
                (df['Data_Status'] == 'ERROR'),
                f.lit(error_statement)
            ).otherwise(
                f.lit('')
            )
        )

        # Holder column is combined into the Error_description and then dropped
        df = df.withColumn(
            'Data_Status_Description',
            f.concat(df['Data_Status_Description'], df[col_name_error_statement])
        ).drop(col_name_error_statement)

    # ==== Remove descriptions which are empty strings -> null
    df = df.withColumn(
        'Data_Status_Description',
        f.when(df['Data_Status_Description'] == f.lit(''), f.lit(None).cast('string'))
        .otherwise(df['Data_Status_Description'])
    )
    return df
