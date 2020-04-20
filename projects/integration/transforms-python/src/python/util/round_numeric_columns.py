# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions

# ===== import: our functions


def round_numeric_columns(df_out, rounded_cols):
    '''
    This function:
        Rounds the cols which are in both df_out and rounded_cols

    Args:
        param1 (dataframe): df_out
        param2 (list): rounded_cols


    Returns:
        dataframe: new dataframe with rounded columns

    '''

    df_cols = df_out.dtypes
    for col, col_type in df_cols:
        if col in rounded_cols and col_type in ['double', 'float']:
            df_out = df_out.withColumn(col,
                f.round(df_out[col], 0)
            )

    return df_out