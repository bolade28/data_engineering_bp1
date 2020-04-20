# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_transform, reorder_columns
from python.pnl.schema.schema_tr160_pnl_output import transformed_output_schema
from python.util.ref_data_error_identification import get_ref_data_status_column
from python.util.round_numeric_columns import round_numeric_columns


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/tr160-pnl_output"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/titan/tr160-pnl_output"),
    ref_data=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes")
)
def dtd_calc(df, ref_data):
    """ Transformation stage function
    Version: T1
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current tr160

    Returns:
        dataframe: transformed tr160
    """

    # === Trader Logic
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date")
    ref_data = ref_data.withColumnRenamed("Portfolio", "Ref_Portfolio").withColumnRenamed("Last_Updated_Timestamp",
                                                                                          "Refdata_Datetime")
    ref_add_cols = ['Bench', 'Team', 'Strategy', 'Trader']

    df_out = compute_transform(df, transformed_output_schema)

    df_out = get_ref_data_status_column(df_out, ref_data, 'Portfolio_Attribute',
                                        ref_add_cols, 'Ref_Portfolio', 'Refdata_Datetime')
    # === Trader Logic end

    # === Rounding
    round_cols = [
        'DTD_Total_Pf',
        'DTD_Real_Pf',
        'DTD_Unreal_Pf',
        'DTD_Total_Undisc_Pf',
        'DTD_Real_Undisc_Pf',
        'DTD_Unreal_Undisc_Pf',
        'DTD_Total_Tran',
        'DTD_Real_Tran',
        'DTD_Unreal_Tran',
        'DTD_Total_Undisc_Tran',
        'DTD_Real_Undisc_Tran',
        'DTD_Unreal_Undisc_Tran',
        'YTD_Total_Pf',
        'YTD_Real_Pf',
        'YTD_Unreal_Pf',
        'YTD_Total_Undisc_Pf',
        'YTD_Real_Undisc_Pf',
        'YTD_Unreal_Undisc_Pf',
        'YTD_Total_Tran',
        'YTD_Real_Tran',
        'YTD_Unreal_Tran',
        'YTD_Total_Undisc_Tran',
        'YTD_Real_Undisc_Tran',
        'YTD_Unreal_Undisc_Tran',
        'LTD_Total_Pf',
        'LTD_Real_Pf',
        'LTD_Unreal_Pf',
        'LTD_Total_Undisc_Pf',
        'LTD_Real_Undisc_Pf',
        'LTD_Unreal_Undisc_Pf',
        'LTD_Total_Tran',
        'LTD_Real_Tran',
        'LTD_Unreal_Tran',
        'LTD_Total_Undisc_Tran',
        'LTD_Real_Undisc_Tran',
        'LTD_Unreal_Undisc_Tran',
        'MTD_Total_Pf',
        'MTD_Real_Pf',
        'MTD_Unreal_Pf',
        'MTD_Total_Undisc_Pf',
        'MTD_Real_Undisc_Pf',
        'MTD_Unreal_Undisc_Pf',
        'MTD_Total_Tran',
        'MTD_Real_Tran',
        'MTD_Unreal_Tran',
        'MTD_Total_Undisc_Tran',
        'MTD_Real_Undisc_Deal',
        'MTD_Unreal_Undisc_Deal',
        'QTD_Total_Pf',
        'QTD_Real_Pf',
        'QTD_Unreal_Pf',
        'QTD_Total_Undisc_Pf',
        'QTD_Real_Undisc_Pf',
        'QTD_Unreal_Undisc_Pf',
        'QTD_Total_Tran',
        'QTD_Real_Tran',
        'QTD_Unreal_Tran',
        'QTD_Total_Undisc_Tran',
        'QTD_Real_Undisc_Tran',
        'QTD_Unreal_Undisc_Tran'
    ]
    df_out = round_numeric_columns(df_out, round_cols)
    # === Rounding end

    # Reorder according to business requirements
    df_out = reorder_columns(df_out, transformed_output_schema)

    df_out = df_out.withColumnRenamed("Portfolio", "Book")
    return df_out