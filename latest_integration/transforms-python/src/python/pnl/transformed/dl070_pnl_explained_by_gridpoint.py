# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_transform, reorder_columns
from python.pnl.schema.schema_dl070_pnl_explained_by_gridpoint import transformed_output_schema
from python.util.ref_data_error_identification import get_ref_data_status_column
from python.util.transforms_util import perform_rounding

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def dl070_transformed(df, ref_data):
    ref_data = ref_data.withColumnRenamed("Portfolio", "Ref_Portfolio").withColumnRenamed("Last_Updated_Timestamp",
                                                                                          "Refdata_Datetime")
    ref_add_cols = ['Bench', 'Team', 'Strategy', 'Trader']
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date")

    df_out = compute_transform(df, transformed_output_schema)

    df_out = perform_rounding(df_out, transformed_output_schema)

    df_out = get_ref_data_status_column(df_out, ref_data, 'Portfolio_Attribute',
                                        ref_add_cols, 'Ref_Portfolio', 'Refdata_Datetime')

    # Reorder according to business requirements
    df_out = reorder_columns(df_out, transformed_output_schema)
    return df_out


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl070_pnl_explained_by_gridpoint"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl070-pnl_explained_by_gridpoint"),
    ref_data=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes"),
)
def dtd_calc(df, ref_data):
    """ Transformation stage function
    Version: T1
    This function:
        Transforms dl070 current
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl070

    Returns:
        dataframe: transformed dl070
    """
    df_out = dl070_transformed(df, ref_data)
    
    return df_out
