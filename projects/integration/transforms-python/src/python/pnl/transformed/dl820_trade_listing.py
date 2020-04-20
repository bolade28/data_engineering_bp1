# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.schema_utils import reorder_columns
from python.pnl.schema.schema_dl820_trade_listing import transformed_output_schema


def dl820_transformed(df, ref_data):
    df = df.withColumnRenamed("Reval_Date", "Valuation_Date") \
        .withColumnRenamed("Internal_Portfolio", "Portfolio")
    ref_data = ref_data.select("Start_Date",
                               "End_Date", "Party_Short_Name", "Party_Long_Name", "Last_Updated_Timestamp")
    ref_data = ref_data.withColumnRenamed("End_Date", "RefEnd_Date") \
        .withColumnRenamed("Start_Date", "RefStart_Date") \
        .withColumnRenamed("Party_Long_Name", "Legal_Entity_Name")  # name chnge will supress the name change after join

    # Lookup to get value from ref data
    df = df.join(ref_data, (df['Internal_Business_Unit'] == ref_data['Party_Short_Name']) &
                 (ref_data.RefStart_Date <= df.Valuation_Date) &
                 ((ref_data.RefEnd_Date >= df.Valuation_Date) | (ref_data.RefEnd_Date.isNull())), 'left')

    ref_data = ref_data.withColumnRenamed("RefEnd_Date", "RefEnd_Date1") \
        .withColumnRenamed("RefStart_Date", "RefStart_Date1") \
        .withColumnRenamed("Party_Short_Name", "Party_Short_Name1") \
        .withColumnRenamed("Legal_Entity_Name", "Counterparty_Name") \
        .withColumnRenamed("Last_Updated_Timestamp", "Last_Updated_Timestamp1") \

    df = df.join(ref_data, (df['External_Business_Unit'] == ref_data['Party_Short_Name1']) &
                 (ref_data.RefStart_Date1 <= df.Valuation_Date) &
                 ((ref_data.RefEnd_Date1 >= df.Valuation_Date) | (ref_data.RefEnd_Date1.isNull())), 'left')

    df = df.withColumn("Legal_Entity_Name_Null", f.when(df.Legal_Entity_Name.isNull(),
                       f.lit("Look up of Legal_Entity_Name on Endur_Counterparties failed\n"))
                       .otherwise(f.lit("")))

    df = df.withColumn("Counterparty_Name_Null", f.when(df.Counterparty_Name.isNull(),
                       f.lit("Look up of Counterparty_Name on Endur_Counterparties failed\n"))
                       .otherwise(f.lit("")))
    df = df.withColumn("Data_Status_Description", f.concat(df.Legal_Entity_Name_Null, df.Counterparty_Name_Null))
    df = df.withColumn("Data_Status", f.when(((df.Data_Status_Description == "")), "OK")
                       .otherwise("ERROR"))
    df = df.withColumnRenamed('Last_Updated_Timestamp', 'Refdata_Datetime')
    # Reorder according to business requirements
    df = reorder_columns(df, transformed_output_schema)
    return df


@transform_df(
    Output("/BP/IST-IG-DD/data/technical/pnl/transformed/dl820_trade_listing"),
    df=Input("/BP/IST-IG-DD/data/technical/pnl/current/endur/dl820_trade_listing"),
    ref_data=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Counterparties"),
)
def dtd_calc(df, ref_data):
    """ Transformation stage function
    This function:
        Version: T1
        Renames the columns of the typed dataframe
        Reorders the dataframe based on its respective output schema

    Args:
        params1 (dataframe): current dl820

    Returns:
        dataframe: transformed dl820
    """
    df = dl820_transformed(df, ref_data)
    return df
