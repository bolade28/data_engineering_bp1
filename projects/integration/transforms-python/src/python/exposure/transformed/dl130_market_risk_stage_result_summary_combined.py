# ===== import: python function
from datetime import date
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import compute_transform, reorder_columns
from python.exposure.schema_exposure.schema_dl130_market_risk_stage_result_summary import combined_output_schema
from python.util.transforms_util import *


def convert(df, df_ref_uom_conversions, df_ref_conformed_units, df_endur_curves, df_endur_option_curves):
    df_ref_uom_conversions = (df_ref_uom_conversions
                .withColumnRenamed('From_Unit', 'From_Unit_ref_conver')
                .withColumnRenamed('To_Unit', 'To_Unit_ref_conver')
                .withColumnRenamed('Conversion', 'Conversion_ref_conver')
                .withColumnRenamed('Unit_Type', 'Unit_Type_ref_conver')
                .withColumnRenamed('Start_Date', 'Start_Date_ref_conver')
                .withColumnRenamed('End_Date', 'End_Date_ref_conver')
                .withColumnRenamed('Late_Updated_Timestamp', 'Late_Updated_Timestamp_ref_conver')
                .withColumnRenamed('Last_Updated_User', 'Last_Updated_User_ref_conver')
    )

    df_endur_option_curves = (df_endur_option_curves
            .withColumnRenamed('Instrument_Type', 'Instrument_Type_ref_options')
            .withColumnRenamed('Risk_Factor', 'Risk_Factor_ref_options')
            .withColumnRenamed('Display_Name', 'Display_Name_ref_options')
            .withColumnRenamed('Fobus', 'Fobus_ref_options')
            .withColumnRenamed('Limits', 'Limits_ref_options')
            .withColumnRenamed('Start_Date', 'Start_Date_ref_options')
            .withColumnRenamed('End_Date', 'End_Date_ref_options')
            .withColumnRenamed('Late_Updated_Timestamp', 'Late_Updated_Timestamp_ref_options')
            .withColumnRenamed('Last_Updated_User', 'Last_Updated_User_ref_options')
    )

    df_ref_conformed_units = (df_ref_conformed_units
                .withColumnRenamed('BU', 'BU_ref_conf')
                .withColumnRenamed('From_Unit', 'From_Unit_ref_conf')
                .withColumnRenamed('Conformed_Unit', 'Conformed_Unit_ref_conf')
                .withColumnRenamed('Start_Date', 'Start_Date_ref_conf')
                .withColumnRenamed('End_Date', 'End_Date_ref_conf')
                .withColumnRenamed('Late_Updated_Timestamp', 'Late_Updated_Timestamp_ref_conf')
                .withColumnRenamed('Last_Updated_User', 'Last_Updated_User_ref_conf')
    )

    df_endur_curves = (df_endur_curves
                .withColumnRenamed('Source_Curve_Name', 'Source_Curve_Name_ref_curves')
                .withColumnRenamed('Curve_Name', 'Curve_Name_ref_curves')
                .withColumnRenamed('Fobus_Curve_Name', 'Fobus_Curve_Name_ref_curves')
                .withColumnRenamed('Limits_Group', 'Limits_Group_ref_curves')
                .withColumnRenamed('Start_Date', 'Start_Date_ref_curves')
                .withColumnRenamed('End_Date', 'End_Date_ref_curves')
                .withColumnRenamed('Late_Updated_Timestamp', 'Late_Updated_Timestamp_ref_curves')
                .withColumnRenamed('Last_Updated_User', 'Last_Updated_User_ref_curves')
    )

    df = compute_constants_and_rename(df, combined_output_schema)

    df = df.withColumn('Projection_Index_tmp',
            f.when(
                (df['Projection_Index'].isNull()),
                df['Risk_Factor_Name']
            )
            .otherwise(
                df['Projection_Index']
            )
        ).drop(
            'Projection_Index'
        ).withColumnRenamed(
            'Projection_Index_tmp', 'Projection_Index'
        )


    df = df.join(f.broadcast(df_endur_option_curves), 
        ((df_endur_option_curves['Instrument_Type_ref_options'] == df['Instrument_Type']) & 
            (df_endur_option_curves['Risk_Factor_ref_options'] == df['Risk_Factor_Name'])
        ),
        'left'
    )

    df = df.join(f.broadcast(df_endur_curves),
            (df_endur_curves['Source_Curve_Name_ref_curves'] == df['Projection_Index']), #TODO: Do  you want projection_Index -> curve index
            'left'
        )

    df = df.withColumn('Limits_Group',
        f.when(
            df['Limits_ref_options'].isNull(),
            f.when(
                df['Curve_Name_ref_curves'].isNotNull(),
                df['Curve_Name_ref_curves']
            )
            .otherwise(
                f.lit('unmapped')
            )
        )
        .otherwise(
            df['Limits_ref_options']
        )
    )

    df = df.join(df_ref_conformed_units, 
        ((df_ref_conformed_units['From_Unit_ref_conf'] == df['MR_Delta_UOM']) & 
            (df_ref_conformed_units['BU_ref_conf'] == 'LNG')
        ),
        'left'
    )

    df = df.join(
        df_ref_uom_conversions,
        ((df_ref_uom_conversions['From_Unit_ref_conver'] == df['MR_Delta_UOM']) & 
            (df_ref_uom_conversions['To_Unit_ref_conver'] == df['Conformed_Unit_ref_conf'])
        ),
        'left'
    )

    # MR_Delta_Conformed
    df = df.withColumn('MR_Delta_Conformed',
        f.when(
            ((df['MR_Delta_UOM'] == 'EUR') | (df['MR_Delta_UOM'] == 'GBP')),
            df['MR_Delta']
        )
        .otherwise(
            df['MR_Delta'] * df['Conversion_ref_conver']
        )
    )
    df = df.withColumn('MR_Delta',
        f.round(df['MR_Delta'], 0)
    )

    # Conversion_Rate
    df = df.withColumn('Conversion_Rate',
        f.when(
            ((df['MR_Delta_UOM'] == 'EUR') | (df['MR_Delta_UOM'] == 'GBP')),
            f.lit(1)
        )
        .otherwise(
            df['Conversion_ref_conver']
        )
    )

    # MR_Delta_Conformed_UOM
    df = df.withColumn('MR_Delta_Conformed_UOM',
        f.when(
            ((df['MR_Delta_UOM'] == 'EUR') | (df['MR_Delta_UOM'] == 'GBP')),
            df['MR_Delta_UOM']
        )
        .otherwise(
            df['Conformed_Unit_ref_conf']
        )
    )

    df = df.withColumn('Trade_Status',
        f.when(
            df['Bucket_End_Date'] >= date.today(),
            f.lit('Active')
        )
    )

    null_string_col = [
        'Counterparty',
        'Price_UOM',
        'Trade_Date',
        'Deal_Number',
        'Product_Type',
        'Delta_Unit_Type',
        'Cargo_Delivery_Location',
        'Cargo_Reference',
        'Titan_Strategy'
    ]
    for col_name in null_string_col:
        df = df.withColumn(col_name, f.lit(None).cast(StringType()))

    null_int_col = [
        'Cargo_Delivery_Month',
        'Cargo_Delivery_Year'
    ]
    for col_name in null_int_col:
        df = df.withColumn(col_name, f.lit(None).cast(IntegerType()))

    df = df.withColumn('Price', f.lit(0.))

    df = df.withColumn('Liquid_Window', f.lit('Yes'))

    df_res = compute_transform(df, combined_output_schema)

    # Reorder according to business requirements
    df_res = reorder_columns(df_res, combined_output_schema)
    return df_res


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/exposure/transformed/dl130_Endur_Monthly_Exposure_combined_Format"),
    df=Input("/BP/IST-IG-DD/data/technical/exposure/transformed/dl130_market_risk_stage_result_summary_conformed_optimised"),
    df_ref_uom_conversions=Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions"),
    df_ref_conformed_units=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units"),
    df_endur_curves=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Curves"),
    df_endur_option_curves=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Options_Curves"),
)
def my_compute_function(df, df_ref_uom_conversions, df_ref_conformed_units, df_endur_curves, df_endur_option_curves):
    return convert(df, df_ref_uom_conversions, df_ref_conformed_units, df_endur_curves, df_endur_option_curves)