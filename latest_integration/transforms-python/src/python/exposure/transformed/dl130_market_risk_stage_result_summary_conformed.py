# ===== import: python function
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.exposure.schema_exposure.schema_dl130_market_risk_stage_result_summary import conformed_output_schema
from python.util.schema_utils import compute_transform, reorder_columns
from python.util.ref_data_error_identification import get_ref_data_status_column
from python.util.round_numeric_columns import round_numeric_columns
from python.util.exposure_util import add_grid_point_month


def convert(df, df_uom_conversions, df_endur_counterparties, df_portfolio):

    # === Trader Logic
    df = df.withColumnRenamed("Analysis_Date", "Valuation_Date")
    ref_data = df_portfolio.withColumnRenamed("Portfolio_ref_port", "Ref_Portfolio").withColumnRenamed("Last_Updated_Timestamp",
                                                                                          "Refdata_Datetime")
    ref_add_cols = ['Bench', 'Team', 'Strategy', 'Trader']

    df = get_ref_data_status_column(df, ref_data, 'Portfolio_Attribute',
                                        ref_add_cols, 'Ref_Portfolio', 'Refdata_Datetime')
    # === Trader Logic end
    
    df = df.join(
        f.broadcast(df_endur_counterparties.select('Party_Short_Name_ref_counter', 'Party_Long_Name_ref_counter')), 
        df['Internal_BU'] == df_endur_counterparties['Party_Short_Name_ref_counter'],
        'left'
    ).withColumnRenamed('Party_Long_Name_ref_counter', 'Legal_Entity_Name')


    df = df.withColumn('MR_Delta_UOM',
            f.when(
                (f.col('UOM') == 'Currency') & (f.col('Risk_Factor_Name') == 'FX_EUR'), 'EUR'
            )
            .when(
                (f.col('UOM') == 'Currency') & (f.col('Risk_Factor_Name') == 'FX_GBP'), 'GBP'
            )
            .otherwise(
                f.col('UOM')
            )
        )

    df = MR_Delta_function(df, 'MR_Delta_Therms', 'Therms', df_uom_conversions)
    df = MR_Delta_function(df, 'MR_Delta_MWh', 'MWh', df_uom_conversions)
    df = MR_Delta_function(df, 'MR_Delta_MMBtu', 'MMBtu', df_uom_conversions)

    df = (
        df.withColumn('Bucket_Month',
            f.date_format(df['Bucket_End_Date'], 'MMM yyyy')
        )
        .withColumn('Bucket_Quarter',
            f.lit(
                f.concat(
                    (f.date_format(df['Bucket_End_Date'], 'yyyy ')),
                    f.lit('Q'),
                    (f.quarter(df['Bucket_End_Date']))
                )
            )
        )
        .withColumn('Bucket_Season', # April 2019 to September 2019 = 2019 S, October 2019 - March 2020 = 2019 W (S = summer, W = winter)
            f.lit(
                f.concat(
                    (f.date_format(f.add_months(df['Bucket_End_Date'], -3), 'yyyy ')),
                    f.when(
                        f.month(f.add_months(df['Bucket_End_Date'], -3)) > 6,
                        f.lit("W")
                    )
                    .otherwise(
                        f.lit("S")
                    )
                )
            )
        )
        .withColumn('Bucket_Year',
            f.date_format(df['Bucket_End_Date'], 'yyyy')
        )
    )

    df = df.withColumn('Bucket_Id', df['Bucket_Id'].cast(IntegerType()))
    df = df.withColumn('Grid_Point_ID', df['Grid_Point_ID'].cast(IntegerType()))
    df = add_grid_point_month(df)

    df = df.join(df_uom_conversions.select('From_Unit_ref_conver', 'To_Unit_ref_conver', 'Conversion_ref_conver'),
            ((df_uom_conversions['From_Unit_ref_conver'] == df['MR_Delta_UOM']) & (df_uom_conversions['To_Unit_ref_conver'] == 'BBL')),
            'left'
        ).drop('From_Unit_ref_conver', 'To_Unit_ref_conver')
    df = df.withColumn('MR_Delta_BBL',
        f.round(
            (df['MR_Delta'] * df['Conversion_ref_conver']),
            0
        )
    )
    df = df.na.fill({'MR_Delta_BBL': 0})

    df = (
        df.withColumn('MR_Delta_EUR',
            f.when(df['Risk_Factor_Name'] == 'FX_EUR', f.round(df['MR_Delta'], 0))
        )
        .withColumn('MR_Delta_GBP',
            f.when(df['Risk_Factor_Name'] == 'FX_GBP', f.round(df['MR_Delta'], 0))
        )
    )


    ref_list = [
        [df_uom_conversions,'Last_Updated_Timestamp'],
        [df_endur_counterparties, 'Last_Updated_Timestamp'],
        [df_portfolio, 'Last_Updated_Timestamp']
    ]

    df = df.withColumn("RefData_Datetime", 
        f.lit(get_greatest_of_all(ref_list))
    )

    df = compute_transform(df, conformed_output_schema)

    # Reorder according to business requirements
    df = reorder_columns(df, conformed_output_schema)

    # Rounding
    round_cols = [
        'MR_Delta',
        'MR_Gamma',
        'MR_Vega',
        'MR_Theta',
        'Market_Value',
        'Market_Value'
    ]
    df = round_numeric_columns(df, round_cols)

    return df


def get_greatest_of_all(ref_list):
    greatest_of_all = 0
    for ref_data in ref_list:
        df = ref_data[0]
        col = ref_data[1]
        current_greatest = df.agg({col: "max"}).collect()[0][0]
        if greatest_of_all == 0:
            greatest_of_all = current_greatest
        if greatest_of_all < current_greatest:
            greatest_of_all = current_greatest
    return greatest_of_all


def MR_Delta_function(df, col_name, to_unit, df_uom_conversions):
    df = df.withColumn(col_name, f.lit(0.))
    df_MMbtu = df.filter(df['Risk_Factor_Name'] == f.lit('MMBtu'))
    df_MMbtu = df_MMbtu.withColumn(col_name,
        f.round(df_MMbtu['MR_Delta'], 0)
    )


    df_API2 = df.filter(df['Risk_Factor_Name'] == f.lit('API2'))
    df_API2 = df_API2.withColumn(col_name,
        f.round(
            (df_API2['MR_Delta'] * f.lit(get_value_from_ref_conv(df_uom_conversions, f.lit('Tonnes (Coal)'), to_unit, 'Conversion_ref_conver'))),
            0
        )
    )
    df_API2 = df_API2.na.fill({col_name: 0})
    df_API2 = df_API2.withColumn(col_name,
        f.lit(0.)
    )

    df_non = df.filter((df['Risk_Factor_Name'] != 'API2')& (df['Risk_Factor_Name'] != to_unit))
    df_non = df_non.join(df_uom_conversions.select('From_Unit_ref_conver','To_Unit_ref_conver', 'Conversion_ref_conver'),
        (df_uom_conversions['From_Unit_ref_conver'] == df_non['UOM']) & (df_uom_conversions['To_Unit_ref_conver'] == to_unit),
        'left'
    ).drop('From_Unit_ref_conver','To_Unit_ref_conver')
    df_non = df_non.withColumn(col_name,
        f.round((df_non['MR_Delta'] * df_non['Conversion_ref_conver']),0)
    ).drop('Conversion_ref_conver')


    df_output = df_MMbtu.union(df_non)
    df_output = df_output.union(df_API2)
    return df_output


def get_value_from_ref_conv(df_ref_conv, from_unit, to_unit, output_col):
    try:
        output_value = df_ref_conv.select(output_col).filter(
            ((df_ref_conv['From_Unit_ref_conver'] == from_unit) & (df_ref_conv['To_Unit_ref_conver'] == to_unit))
        ).distinct().take(1)[0][0]
        return output_value
    except Exception:
        return None # 1063702 TODO: might cause future bugs

@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/transformed/dl130_market_risk_stage_result_summary_conformed_optimised"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/current/endur/dl130_market_risk_stage_result_summary"),
    df_endur_counterparties=Input('/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Counterparties'),
    df_uom_conversions=Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions"),
    df_portfolio=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Portfolio_Attributes"),
)
def my_compute_function(df, df_uom_conversions, df_endur_counterparties, df_portfolio):
    df_uom_conversions = (df_uom_conversions
            .withColumnRenamed('From_Unit', 'From_Unit_ref_conver')
            .withColumnRenamed('To_Unit', 'To_Unit_ref_conver')
            .withColumnRenamed('Conversion', 'Conversion_ref_conver')
            .withColumnRenamed('Unit_Type', 'Unit_Type_ref_conver')
            .withColumnRenamed('Start_Date', 'Start_Date_ref_conver')
            .withColumnRenamed('End_Date', 'End_Date_ref_conver')
    )

    df_endur_counterparties = (df_endur_counterparties
            .withColumnRenamed('Run_Datetime', 'Run_Datetime_ref_counter')
            .withColumnRenamed('Party_Long_Name', 'Party_Long_Name_ref_counter')
            .withColumnRenamed('Party_Short_Name', 'Party_Short_Name_ref_counter')
            .withColumnRenamed('ICOS_ID', 'ICOS_ID_ref_counter')
            .withColumnRenamed('Party_Type', 'Party_Type_ref_counter')
            .withColumnRenamed('Start_Date', 'Start_Date_ref_counter')
            .withColumnRenamed('End_Date', 'End_Date_ref_counter')
    )

    df_portfolio = (df_portfolio
            .withColumnRenamed('Portfolio', 'Portfolio_ref_port')
    )

    return convert(df, df_uom_conversions, df_endur_counterparties, df_portfolio)
