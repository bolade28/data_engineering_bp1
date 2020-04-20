import pyspark.sql.functions as f
from transforms.api import transform_df, Input, Output

from python.util.transforms_util import error_checkers, cal_final_data_status,\
                                        perform_rounding, reorder_columns, copy_field_value,\
                                        calc_bucket_logic, overwrite_field_content,\
                                        compute_constants_and_rename, compute_liquid_window,\
                                        ref_data_join, cal_RefData_Datetime, calc_data_status_desc\


from python.exposure.schema_exposure.schema_fp_gas_m0145_combined_detail import transformed_output_schema
from python.util.exposure_util import add_grid_point_month


def compute_gbp_eur(df, field, currency):
    df = df.withColumn(field, f.when((f.col('UOM') == currency), f.col('MR_Delta'))
                               .otherwise(f.lit(0.)))
    return df


def convert(df, ref_data_UOM_Conversions, ref_data_NAGP_Curves, ref_data_NAGP_Counterparties,
            df_ref_conformed_units, df_ref_nagp_uoms, df_ref_portf_attr):
    ref_data_NAGP_Curves = f.broadcast(ref_data_NAGP_Curves)
    ref_data_NAGP_Counterparties = f.broadcast(ref_data_NAGP_Counterparties)
    ref_data_UOM_Conversions = f.broadcast(ref_data_UOM_Conversions)
    ref_conformed_units = f.broadcast(df_ref_conformed_units)
    ref_nagp_uoms = f.broadcast(df_ref_nagp_uoms)
    ref_portf_attr = f.broadcast(df_ref_portf_attr)

    df = df.drop("Grid_Point_Start_Date")\
           .drop("Grid_Point_End_Date")
    df = compute_constants_and_rename(df, transformed_output_schema)
    df = add_grid_point_month(df)
    df = calc_bucket_logic(df)

    df = df.withColumn("Trade_Status", f.when(f.current_date()
                                              > f.col("Grid_Point_End_Date"), "Active").otherwise(f.lit(None)))

    df = compute_liquid_window(df, 'Grid_Point_Start_Date', 'Valuation_Date', 'Liquid_Window')

    df = ref_data_join(
        df=df,
        df_ref=ref_conformed_units,
        data_source='UOM',
        ref_source='From_Unit',
        lookup_columns={
            'From_Unit': 'From_Unit_0',
            'Conformed_Unit': 'Conformed_Unit',
            'BU': 'BU',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_0',
        },
    )

    df = error_checkers(df, 'ref_conformed_units', 'Conformed_Unit', 'BU')

    df = ref_data_join(
        df=df,
        df_ref=ref_data_UOM_Conversions,
        data_source='From_Unit_0',
        ref_source='From_Unit',
        lookup_columns={
            'From_Unit': 'From_Unit',
            'To_Unit': 'To_Unit',
            'Conversion': 'Conversion',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_1',
        },
    )

    df = df.filter(df['To_Unit'] == df['Conformed_Unit'])

    df = df.withColumn('Conversion_Rate',
                       f.when(df['UOM'].isin("DAYS MRA", "MWH", "EUR", "GBP"), f.lit(1))
                        .otherwise(df['Conversion']))

    df = df.withColumn('MR_Delta_MWh', f.when((df['Risk_Factor_UOM'] == 'MMBtu') | 
                                              (df['Risk_Factor_UOM'] == 'MMBTU'), df['MR_Delta'])
                       .when((df['UOM'] == df['From_Unit']) & ((df['To_Unit'] == 'MWh') | (df['To_Unit'] == 'MWH')),
                             df['MR_Delta'] * df['Conversion'])
                       .otherwise(0))
 
    df = df.withColumn('MR_Delta_Therms', f.when((df['Risk_Factor_UOM'] == 'MMBtu') |
                                                 (df['Risk_Factor_UOM'] == 'MMBTU'), df['MR_Delta'])
                       .when((df['UOM'] == df['From_Unit']) & ((df['To_Unit'] == 'MWh') | (df['To_Unit'] == 'Therms')),
                             df['MR_Delta'] * df['Conversion'])
                       .otherwise(0))

    df = df.withColumn('MR_Delta_MMBtu', f.when((df['Risk_Factor_UOM'] == 'MMBtu') |
                                                (df['Risk_Factor_UOM'] == 'MMBTU'), df['MR_Delta'])
                       .when((df['UOM'] == df['From_Unit']) & ((df['To_Unit'] == 'MWh') | (df['To_Unit'] == 'MMBtu')),
                             df['MR_Delta'] * df['Conversion'])
                       .otherwise(0))

    df = df.withColumn('MR_Delta_BBL', f.when((df['UOM'] == df['From_Unit']) & ((df['To_Unit'] == 'BBL') | (df['To_Unit'] == 'bbl')),
                                              df['MR_Delta'] * df['Conversion'])
                                        .otherwise(0))

    df = compute_gbp_eur(df, 'MR_Delta_GBP', 'GBP')
    df = compute_gbp_eur(df, 'MR_Delta_EUR', 'EUR')

    df = df.withColumn("MR_Delta_Conformed",  f.round(df["MR_Delta"] * (df["Conversion_Rate"]), 0))
    df = df.withColumn("MR_Delta_Conformed_UOM",  f.round(df["MR_Delta"] * (df["Conversion_Rate"]), 0))

    df = ref_data_join(
        df=df,
        df_ref=ref_data_NAGP_Curves,
        data_source='Risk_Factor_Name',
        ref_source='Source_Curve_Name',
        lookup_columns={
            'Curve_Name': 'Curve_Name',
            'Limits_Group': 'Limits_Group',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_2',
        },
    )

    df = error_checkers(df, 'NAGP_Curves', 'Curve_Name', 'Limits_Group')

    df = copy_field_value(df, transformed_output_schema)

    df = ref_data_join(
        df=df,
        df_ref=ref_nagp_uoms,
        data_source='UOM',
        ref_source='NAGP_Name',
        lookup_columns={
            'UoM_Name': 'MR_Delta_UOM',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_3',
        },
    )

    df = ref_data_join(
        df=df,
        df_ref=ref_data_NAGP_Counterparties,
        data_source='Internal_BU',
        ref_source='Short_Name',
        lookup_columns={
            'Legal_Entity_Name': 'Legal_Entity_Name',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_4',
        },
    )

    df = error_checkers(df, 'NAGP_Counterparties', 'Legal_Entity_Name')

    df = ref_data_join(
        df=df,
        df_ref=ref_data_NAGP_Counterparties,
        data_source='External_BU',
        ref_source='Short_Name',
        lookup_columns={
            'Legal_Entity_Name': 'Counterparty',
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_5',
        },
    )

    df = error_checkers(df, 'NAGP_Counterparties', 'Counterparty')

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
            'Last_Updated_Timestamp': 'Last_Updated_Timestamp_6',
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
        Output("/BP/IST-IG-DD/data/technical/exposure/transformed/fp_gas_m0145_combined_detail"),
        df=Input("/BP/IST-IG-DD/data/technical/exposure/current/freeport/fp_gas_m0145"),
        df_ref_uom_conversions=Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions"),
        df_ref_nagp_curves=Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Curves"),
        df_ref_counterparties=Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Counterparties"),
        df_ref_conformed_units=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units"),
        df_ref_nagp_uoms=Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_UOMs"),
        df_ref_portf_attr=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes")
)
def myComputeFunction(
        df,	df_ref_uom_conversions, 	df_ref_nagp_curves, df_ref_counterparties,
        df_ref_conformed_units, df_ref_nagp_uoms, df_ref_portf_attr
        ):
    """
    Transformation stage function
    Version: T2
    This function:
        For performing lookups, transform data, rename columns and
        reports any error when performing lookup.
        Reorders the dataframe based on its respective output schema

    Args:
        df (dataframe): current fp_gas_145m
        df_ref_uom_conversions (dataframe): UOM_Conversion
        df_ref_nagp_curves (dataframe): NAGP_Curves
        df_ref_counterparties (dataframe): NAGP_Counterparties
        df_ref_conformed_units (dataframe): Conformed_Units
        df_ref_nagp_uoms (dataframe): NAGP_UoMs
        df_ref_portf_attr (dataframe): Portfolio Attributes

    Returns:
        dataframe: transformed fp_gas_145m
    """
    return convert(
        df, df_ref_uom_conversions,	df_ref_nagp_curves, df_ref_counterparties,
        df_ref_conformed_units, df_ref_nagp_uoms, df_ref_portf_attr
        )
