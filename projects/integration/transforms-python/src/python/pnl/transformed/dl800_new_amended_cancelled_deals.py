# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_transform, reorder_columns
from python.pnl.schema.schema_dl800_new_amended_cancelled_deals import transformed_output_schema
from python.util.ref_data_error_identification import get_ref_data_status_column
from python.util.round_numeric_columns import round_numeric_columns


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl800_new_amended_cancelled_deals"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl800_new_amended_cancelled_deals"),
    ref_portfolio=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes"),
    ref_counterparties=Input('/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Counterparties'),
)
def dtd_calc(df, ref_portfolio, ref_counterparties):
    """ Transformation stage function
    Version: T1
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl800

    Returns:
        dataframe: transformed dl800
    """
    df = compute_transform(df, transformed_output_schema)

    ref_portfolio = ref_portfolio.withColumnRenamed("Start_Date", "portfolio_Start_Date")\
                                 .withColumnRenamed("End_Date", "portfolio_End_Date")\
                                 .withColumnRenamed("Portfolio", "attributes_Portfolio")\
                                 .withColumnRenamed("Last_Updated_Timestamp", "Refdata_Datetime")\
                                 .withColumnRenamed("Last_Updated_User", "attributes_Last_Updated_User")

    ref_counterparties = ref_counterparties.withColumnRenamed("Start_Date", "attributes_Start_Date")\
                                           .withColumnRenamed("End_Date", "attributes_End_Date")\
                                           .withColumnRenamed("Last_Updated_Timestamp", "attributes_Last_Updated")\
                                           .withColumnRenamed("Last_Updated_User", "attributes_Last_Updated_User")

    df = df.join(f.broadcast(ref_counterparties.select([f.col('Party_Long_Name').alias("Counterparty"),
                                                        'Party_Short_Name',
                                                        'attributes_Start_Date',
                                                        'attributes_End_Date'])),
                 (ref_counterparties.Party_Short_Name == df.External_Business_Unit) &
                 (ref_counterparties['attributes_Start_Date'] <= df['Valuation_Date']) &
                 ((df['Valuation_Date'] <= ref_counterparties['attributes_End_Date']) |
                 ref_counterparties['attributes_End_Date'].isNull()),
                 how='left')

    df = df.alias("data").join(ref_counterparties.alias("ref")["Party_Long_Name",
                                                               "Party_Short_Name",
                                                               'attributes_Start_Date',
                                                               'attributes_End_Date'],
                               (f.col("data.Internal_Business_Unit") == f.col("ref.Party_Short_Name"))&
                               (ref_counterparties['attributes_Start_Date'] <= df['Valuation_Date']) &
                               ((df['Valuation_Date'] <= ref_counterparties['attributes_End_Date']) |
                               ref_counterparties['attributes_End_Date'].isNull()),
                               how="left")

    df = df.withColumnRenamed("Party_Long_Name", "Legal_Entity")

    df = df.withColumnRenamed("Trader", "ingest_Trader")

    ref_add_cols = ['Bench', 'Team', 'Strategy', 'Trader']
    df_out = get_ref_data_status_column(df, ref_portfolio, 'Portfolio_Atributes',
                                        ref_add_cols, 'attributes_Portfolio', 'Refdata_Datetime',
                                        start_date_col="portfolio_Start_Date",
                                        end_date_col="portfolio_End_Date")

    round_col = ['LTD_Total_Pf']
    df_out = round_numeric_columns(df_out, round_col)

    # Reorder according to business requirements
    df_out = reorder_columns(df_out, transformed_output_schema)

    return df_out
