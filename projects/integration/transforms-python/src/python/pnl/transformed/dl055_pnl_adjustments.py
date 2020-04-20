# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
import python.util.bp_reorder as bp_reorder
from python.pnl.schema.schema_dl055_pnl_adjustments import transformed_output_schema


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl055_pnl_adjustments"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl055-pnl_adjustments"),
)
def dtd_calc(df):
    """ Transformation stage function
    Version: T0
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl055

    Returns:
        dataframe: transformed dl055
    """
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date") \
        .withColumnRenamed("Pfolio_CCY", "Pf_CCY") \
        .withColumnRenamed("Daily_Adj_Value_Pfolio_CCY", "DTD_Disc_Total_Adj_Pf") \
        .withColumnRenamed("Monthly_Adj_Value_Pfolio_CCY", "MTD_Disc_Total_Adj_Pf") \
        .withColumnRenamed("Yearly_Adj_Value_Pfolio_CCY", "YTD_Disc_Total_Adj_Pf") \
        .withColumnRenamed("Daily_Adj_Value_Adj_CCY", "DTD_Disc_Total_Adj") \
        .withColumnRenamed("Monthly_Adj_Value_Adj_CCY", "MTD_Disc_Total_Adj") \
        .withColumnRenamed("Yearly_Adj_Value_Adj_CCY", "YTD_Disc_Total_Adj")

    reordered_schema = bp_reorder.schema_reader(transformed_output_schema)
    df = df.select(*reordered_schema)

    return df
