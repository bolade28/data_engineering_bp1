# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform, Input, Output, incremental, configure

# ===== import: our functions
from python.pnl.typed import utils
from python.pnl.schema.schema_dl050_pnl_by_portfolio_adjusted import typed_output_schema


@configure(profile=['EXECUTOR_MEMORY_SMALL', 'NUM_EXECUTORS_8'])
@incremental()
@transform(
    output_data=Output("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl050-pnl_by_portfolio_adjusted"),
    input_data=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl050-pnl_by_portfolio_adjusted/dl050-pnl_by_portfolio_adjusted"),
)
def my_compute_function(ctx, input_data, output_data):
    '''
    Version: V1 (Python reader)
    This function:
        1. renames columns
        2. casts the columns to the correct types

    Args:
        params1 (dataframe): raw dl050

    Returns:
        dataframe: typed dl050
    '''

    expected_fields = list(map(lambda x: x.name, typed_output_schema))
    try:
        prev_col_names = output_data.dataframe().columns
    except Exception:
        prev_col_names = []

    df = utils.build_input_data(ctx, input_data, prev_col_names, expected_fields)
    if df:
        df = df.select(
            f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
            f.to_date(df['Reval_Date'], 'yyyy-MM-dd').alias('Reval_Date'),
            df['Portfolio'],
            df['Portfolio_CCY'],
            df['DtD_PF_CCY'].cast('double'),
            df['DtD_Adj_PF_CCY'].cast('double'),
            df['DtD_PF_CCY_and_Adj'].cast('double'),
            df['MtD_PF_CCY'].cast('double'),
            df['MtD_Adj_PF_CCY'].cast('double'),
            df['MtD_PF_CCY_and_Adj'].cast('double'),
            df['YtD_PF_CCY'].cast('double'),
            df['YtD_Adj_PF_CCY'].cast('double'),
            df['YtD_PF_CCY_and_Adj'].cast('double'),
            df['DtD_USD'].cast('double'),
            df['DtD_Adj_USD'].cast('double'),
            df['DtD_USD_and_Adj'].cast('double'),
            df['MtD_USD'].cast('double'),
            df['MtD_Adj_USD'].cast('double'),
            df['MtD_USD_and_Adj'].cast('double'),
            df['YtD_USD'].cast('double'),
            df['YtD_Adj_USD'].cast('double'),
            df['YtD_USD_and_Adj'].cast('double')
        )
        output_data.write_dataframe(df)
