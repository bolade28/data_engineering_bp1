# ===== import: python function
from pyspark.sql import functions as f

# ===== import: palantir functions

# ===== import: our functions

# ===== import: logging
import logging
logger = logging.getLogger(__name__)

COL_UNCOFORMED_UOM = "Unconformed_Curve_Delta_UOM"
COL_COFORMED_UOM = "Curve_Delta_UOM"
COL_UNCOFORMED_VALUE = "Curve_Delta_UOM"
COL_COFORMED_VALUE = "Curve_Delta"
COL_CONVERSION_RATE = "Conversion_Rate"


def addConversionRate(
            df,
            df_uom_conversions,
            colDelta,
            colSourceUnit,
            colTargetUnit,
            colConversionRate,
            throwExceptionIfNotFound
        ):
    df_uom_conversions = f.broadcast(df_uom_conversions
                                             .withColumnRenamed("From_Unit", "Uom_Conversions_From_Unit")
                                             .withColumnRenamed("To_Unit", "Uom_Conversions_To_Unit")
                                             .withColumnRenamed("Conversion", "Uom_Conversions_Conversion")
                                             )
    df = df.join(df_uom_conversions, (df[colSourceUnit] == df_uom_conversions["Uom_Conversions_From_Unit"])
                 & (df[colTargetUnit] == (df_uom_conversions["Uom_Conversions_To_Unit"])), "left")
    df = df.withColumn(colConversionRate,
                       f.coalesce(
                                f.when(df[colSourceUnit] == df[colTargetUnit], f.lit(1.0)),
                                df["Uom_Conversions_Conversion"]
                            )
                       )
    df_missing_rates = df.filter(df[colConversionRate].isNull()).select(
            df[colSourceUnit], df[colTargetUnit]).distinct()
    if (df_missing_rates.count() > 0 and throwExceptionIfNotFound):
        error_list = df_missing_rates.collect()
        logger.error("missing conversion rates for some UOM combinations error_list"+error_list)
    df = df.drop("Uom_Conversions_From_Unit") \
        .drop("Uom_Conversions_To_Unit") \
        .drop("Uom_Conversions_Conversion")
    return df


def addConformedUnit(
        df,
        df_ConformedUnits,
        colUom,
        colConformedUom
        ):
    df_ConformedUnits = f.broadcast(df_ConformedUnits
                                            .withColumnRenamed("From_Unit", "Conformed_Units_From_Unit")
                                            .withColumnRenamed("Conformed_Unit", "Conformed_Units_Conformed_Unit")
                                            .filter(df_ConformedUnits["BU"] == (f.lit("LNG"))).drop("BU")
                                            )
    df = df.join(df_ConformedUnits,  df[colUom] == (df_ConformedUnits["Conformed_Units_From_Unit"]), "left")
    df = df.withColumn(colConformedUom,
                       f.coalesce(df["Conformed_Units_Conformed_Unit"], df[colUom])
                       )
    df = df.drop("Conformed_Units_From_Unit").drop("Conformed_Units_Conformed_Unit")
    return df
