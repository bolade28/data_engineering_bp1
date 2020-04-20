import pyspark.sql.functions as f
from transforms.api import transform_df, Input, Output

from python.util.transforms_util import error_checkers, cal_final_data_status,\
                                        perform_rounding, reorder_columns, copy_field_value,\
                                        calc_bucket_logic, overwrite_field_content,\
                                        compute_constants_and_rename, compute_liquid_window,\
                                        mmbtu_therms_lookup, calc_data_status_desc,\
                                        ref_data_join, cal_RefData_Datetime
from python.exposure.schema_exposure.schema_fp_power_m0145_latest import transformed_output_schema
from python.util.exposure_util import add_grid_point_month


def convert(df, ref_data_UOM_Conversions, ref_data_NAGP_Curves,
            ref_data_NAGP_Counterparties, df_ref_portf_attr):
    ref_data_NAGP_Curves = f.broadcast(ref_data_NAGP_Curves)
    ref_data_NAGP_Counterparties = f.broadcast(ref_data_NAGP_Counterparties)
    ref_data_UOM_Conversions = f.broadcast(ref_data_UOM_Conversions)
    ref_portf_attr = f.broadcast(df_ref_portf_attr)

    df = compute_constants_and_rename(df, transformed_output_schema)

    df = add_grid_point_month(df)
    df = calc_bucket_logic(df)
    df = df.withColumn("Trade_Status", f.when(f.current_date()
                                              > f.col("Grid_Point_End_Date"), "Active").otherwise(f.lit(None)))

    df = compute_liquid_window(df, 'Grid_Point_Start_Date', 'Valuation_Date', 'Liquid_Window')

    out_dict = mmbtu_therms_lookup(ref_data_UOM_Conversions)

    df = ref_data_join(
        df=df,
        df_ref=ref_data_NAGP_Curves,
        data_source='Risk_Factor_Name',
        ref_source='Source_Curve_Name',
        lookup_columns={
            'Curve_Name': 'Curve_Name',
            'Limits_Group': 'Limits_Group',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_1',
        },
    )

    df = error_checkers(df, 'NAGP_Curves', 'Curve_Name', 'Limits_Group')

    df = df.withColumn('MR_Delta_Therms', f.round((df['MR_Delta'] * out_dict['Therms']), 0))
    df = df.withColumn('MR_Delta_MMBtu', f.round((df['MR_Delta'] * out_dict['MMBtu']), 0))

    df = df.withColumn("Grid_Point_Month", f.to_date(f.date_trunc("month", df["Grid_Point_Start_Date"]), "yyyy-MM-dd"))

    df = copy_field_value(df, transformed_output_schema)

    df = ref_data_join(
        df=df,
        df_ref=ref_data_NAGP_Counterparties,
        data_source='External_BU',
        ref_source='Short_Name',
        lookup_columns={
            'Legal_Entity_Name': 'Counterparty',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_2',
        },
    )

    df = error_checkers(df, 'NAGP_Counterparties', 'Counterparty')

    df = ref_data_join(
        df=df,
        df_ref=ref_data_NAGP_Counterparties,
        data_source='Internal_BU',
        ref_source='Short_Name',
        lookup_columns={
            'Legal_Entity_Name': 'Legal_Entity_Name',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_3',
        },
    )

    df = error_checkers(df, 'NAGP_Counterparties', 'Legal_Entity_Name')

    df = df.withColumnRenamed('Trader', 'Deal_Trader')

    df = ref_data_join(
        df=df,
        df_ref=ref_portf_attr,
        data_source='Portfolio/Book',
        ref_source='Portfolio',
        lookup_columns={
            'Portfolio': 'Portfolio',
            'Bench': 'Bench',
            'Team': 'Team',
            'Strategy': 'Strategy',
            'Trader': 'Trader',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_4',
        },
    )

    df = error_checkers(df, 'Portfolio_Attributes', 'Bench', 'Team', 'Strategy', 'Trader')

    df = calc_data_status_desc(
        df, ['BenchNull', 'TeamNull', 'StrategyNull', 'TraderNull', 'CounterpartyNull', 'Legal_Entity_NameNull'])

    df = overwrite_field_content(df, 'Bench', 'Team', 'Strategy', 'Trader', 'Counterparty', 'Legal_Entity_Name')

    df = cal_final_data_status(df)

    df = cal_RefData_Datetime(df)
    df = perform_rounding(df, transformed_output_schema)
    df = reorder_columns(df, transformed_output_schema)

    return df


@transform_df(
        Output("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_power_m0145_combined_detail"),
        df=Input("/BP/IST-IG-DD/data/technical/exposure/current/freeport/fp_power_m0145"),
        df_ref_uom_conversions=Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions"),
        df_ref_nagp_curves=Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Curves"),
        df_ref_counterparties=Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Counterparties"),
        df_ref_portf_attr=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes")
)
def myComputeFunction(
        df,	df_ref_uom_conversions,	df_ref_nagp_curves, df_ref_counterparties, df_ref_portf_attr
        ):
    """
    Transformation stage function
    Version: T2
    This function:
        For performing lookups, transform data, rename columns and
        reports any error when performing lookup.
        Reorders the dataframe based on its respective output schema

    Args:
        df (dataframe): current fp_power_145m
        df_ref_uom_conversions (dataframe): UOM_Conversion
        df_ref_nagp_curves (dataframe): NAGP_Curves
        df_ref_counterparties (dataframe): NAGP_Counterparties
        df_ref_portf_attr (dataframe): Portfolio Attributes

    Returns:
        dataframe: transformed fp_power_145m
    """

    return convert(
        df, df_ref_uom_conversions,	df_ref_nagp_curves, df_ref_counterparties, df_ref_portf_attr
        )
