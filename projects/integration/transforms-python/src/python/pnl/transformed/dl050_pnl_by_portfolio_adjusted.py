# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_transform, reorder_columns
from python.pnl.schema.schema_dl050_pnl_by_portfolio_adjusted import transformed_output_schema
from python.util.ref_data_error_identification import get_ref_data_status_column
from python.util.round_numeric_columns import round_numeric_columns

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def dl050_transformed(df, ref_data):
    df = compute_transform(df, transformed_output_schema)
    ref_data = ref_data.withColumnRenamed("Portfolio", "Ref_Portfolio").withColumnRenamed("Last_Updated_Timestamp",
                                                                                          "Refdata_Datetime")

    ref_add_cols = ['Bench', 'Team', 'Strategy', 'Trader']
    df_out = get_ref_data_status_column(df, ref_data, 'Portfolio_Attribute',
                                        ref_add_cols, 'Ref_Portfolio', 'Refdata_Datetime')

    round_col = [
        'DTD_Total_Pf',
        'DTD_Adj_Pf',
        'DTD_Total_And_Adj_Pf',
        'MTD_Total_Pf',
        'MTD_Adj_Pf',
        'MTD_Total_And_Adj_PF',
        'YTD_Total_Pf',
        'YTD_Adj_Pf',
        'YTD_Total_And_Adj_Pf',
        'DTD_Total_USD',
        'DTD_Adj_USD',
        'DTD_Total_And_Adj_USD',
        'MTD_Total_USD',
        'MTD_Adj_USD',
        'MTD_Total_And_Adj_USD',
        'YTD_Total_USD',
        'YTD_Adj_USD',
        'YTD_Total_And_Adj_USD'
    ]

    df_out = round_numeric_columns(df_out, round_col)

    df_out = reorder_columns(df_out, transformed_output_schema)
    return df_out


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl050_pnl_by_portfolio_adjusted"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl050-pnl_by_portfolio_adjusted"),
    ref_data=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes"),
)
def dtd_calc(df, ref_data):
    """ Transformation stage function
    Version: T1
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl050

    Returns:
        dataframe: transformed dl050
    """
    df_out = dl050_transformed(df, ref_data)
    return df_out
