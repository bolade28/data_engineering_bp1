# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, configure

# ===== import: our functions
from python.util.transforms_util import \
    compute_constants_and_rename, ref_data_join, error_checkers, \
    calc_data_status_desc, reorder_columns, \
    cal_RefData_Datetime, \
    perform_rounding
from python.pnl.schema.schema_dl080_pnl_dmy_by_deal import transformed_output_schema
from python.bp_flush_control import DL080_PROFILE

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def convert(df, df_ref_portf_attr, ref_data_endur_conterparties):
    ref_data_endur_conterparties = f.broadcast(ref_data_endur_conterparties)
    df_ref_portf_attr = f.broadcast(df_ref_portf_attr)

    # Compute all fields that are constants and perform any rename operation.
    df = compute_constants_and_rename(df, transformed_output_schema)

    df = ref_data_join(
        df=df,
        df_ref=ref_data_endur_conterparties,
        data_source='External_Business_Unit',
        ref_source='Party_Short_Name',
        lookup_columns={
            'Party_Long_Name': 'Counterparty',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_1'
        })

    df = ref_data_join(
        df=df,
        df_ref=ref_data_endur_conterparties,
        data_source='Internal_Business_Unit',
        ref_source='Party_Short_Name',
        lookup_columns={
            'Party_Long_Name': 'Legal_Entity',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_2'
        })

    df = ref_data_join(
        df=df,
        df_ref=df_ref_portf_attr,
        data_source='Portfolio',
        ref_source='Portfolio',
        lookup_columns={
            'Bench': 'Bench',
            'Team': 'Team',
            'Strategy': 'Strategy',
            'Trader': 'Trader',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_3'
        })

    df = error_checkers(
        df, 'Portfolio_Attributes', 'Bench', 'Strategy', 'Team', 'Trader', 'Counterparty', 'Legal_Entity')
    df = calc_data_status_desc(
        df, ['BenchNull', 'StrategyNull', 'TeamNull', 'TraderNull', 'CounterpartyNull', 'Legal_EntityNull'])

    df = df.withColumn(
        'Data_Status',
        f.when(df.Data_Status_Description == "", "OK").otherwise("ERROR"))

    df = cal_RefData_Datetime(df)

    df = perform_rounding(df, transformed_output_schema)

    df = reorder_columns(df, transformed_output_schema)

    return df


@configure(profile=DL080_PROFILE)
@transform_df(
        Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl080_pnl_dmy_by_deal"),
        df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl080_pnl_dmy_by_deal"),
        df_ref_portf_attr=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes"),
        df_ref_counterparties=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Counterparties"),
)
def myComputeFunction(df, df_ref_portf_attr, df_ref_counterparties):
    """ Transformation stage function
    Version: T1
    This function:
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl080

    Returns:
        dataframe: transformed dl080
    """
    return convert(df, df_ref_portf_attr, df_ref_counterparties)
