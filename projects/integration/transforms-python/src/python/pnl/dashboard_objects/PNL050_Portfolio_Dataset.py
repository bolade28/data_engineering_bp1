# ===== import: python function
from pyspark.sql import functions as f
from datetime import date

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.round_numeric_columns import round_numeric_columns

GLOBAL_CUT_OFF_DATE = date(2019, 12, 31)
POS_LIMIT = 50000
NEG_LIMIT = -50000

@transform_df(
    Output("/BP/IST-IG-DD/technical/dashboard_objects/pnl/PNL050_Portfolio_Dataset"),
    df_input=Input("/BP/IST-IG-DD/data/published/all/pnl/history/PNL050_By_Portfolio_Adjusted"),
    ref_data_bench=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Object_Bench_Filter")
)
def my_compute_function(df_input, ref_data_bench):
    df = df_input.filter(
        (df_input.Bench.isNotNull()) &
        (df_input.Valuation_Date > GLOBAL_CUT_OFF_DATE)
    )

    df_sorted = df.select(
        df.Valuation_Date,
        df.Portfolio,
        df.Bench,
        df.DTD_Total_And_Adj_USD,
        df.YTD_Total_And_Adj_USD,
        df.MTD_Total_And_Adj_USD
    )
    df_sorted = df_sorted.na.fill(0) #can't sum null values

    output_df = df_sorted.groupBy(df_sorted.Valuation_Date, df_sorted.Bench, df_sorted.Portfolio)
    output_df = output_df.agg(f.sum('YTD_Total_And_Adj_USD').alias('YTD'),
                              f.sum('DTD_Total_And_Adj_USD').alias('DTD'),
                              f.sum('MTD_Total_And_Adj_USD').alias('MTD'))

    output_df = output_df.orderBy(['Valuation_Date', 'Bench', 'Portfolio'], ascending=[0, 1])

    round_cols = [
        'YTD',
        'DTD',
        'MTD'
    ]
    output_df = round_numeric_columns(output_df, round_cols)

    output_df = (output_df
        .withColumn('Commentary_Detail', f.lit(""))
        .withColumn('PK_Portfolio', f.concat(output_df.Bench, f.lit(' '), output_df.Valuation_Date, f.lit(' '), output_df.Portfolio))
        .withColumn('FK_Bench', f.concat(output_df.Bench, f.lit(' '), output_df.Valuation_Date))
    )

    ref_data_bench = ref_data_bench.withColumnRenamed("Bench", "Bench_ref")
    output_df = output_df.withColumn('Data_Status', f.lit("OK"))
    output_df = (output_df.join(ref_data_bench, 
            (output_df['Bench'] == ref_data_bench['Bench_ref'])&
            (ref_data_bench['Start_Date'] <= output_df['Valuation_Date']) &
            ((output_df['Valuation_Date'] <= ref_data_bench['End_Date']) |  ref_data_bench['End_Date'].isNull()), 
            how = "inner"
        )
        .drop('Bench_ref', 'Start_Date', 'End_Date', 'Last_Updated_User')
    )

    output_df = output_df.filter((output_df['DTD'] > POS_LIMIT) | (output_df['DTD'] < NEG_LIMIT))
    return output_df
