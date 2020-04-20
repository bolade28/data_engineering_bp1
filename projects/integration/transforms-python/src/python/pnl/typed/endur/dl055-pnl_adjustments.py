# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, incremental

# ===== import: our functions


@incremental()
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl055-pnl_adjustments"),
    df=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl055-pnl_adjustments/dl055-pnl_adjustments"),
)
def my_compute_function(df):
    '''
    Version: V0
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        params1 (dataframe): raw dl055

    Returns:
        dataframe: typed dl055
    '''
    df = df.select(
        f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
        f.to_date(df['Reval_Date'], 'yyyy-MM-dd').alias('Reval_Date'),
        df['Portfolio'],
        df['Deal_Number'].cast('int'),
        df['Adj_desc'],
        f.to_date(df['Effective_Date'], 'yyyy-MM-dd').alias('Effective_Date'),
        f.to_date(df['End_Date'], 'yyyy-MM-dd').alias('End_Date'),
        df['Pfolio_CCY'],
        df['Daily_Adj_Value_Pfolio_CCY'].cast('double'),
        df['Monthly_Adj_Value_Pfolio_CCY'].cast('double'),
        df['Yearly_Adj_Value_Pfolio_CCY'].cast('double'),
        df['Adj_CCY'],
        df['Daily_Adj_Value_Adj_CCY'].cast('double'),
        df['Monthly_Adj_Value_Adj_CCY'].cast('double'),
        df['Yearly_Adj_Value_Adj_CCY'].cast('double'),
    )
    return df
