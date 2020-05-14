# ===== import: python function
from pyspark.sql import functions as f
from datetime import date

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions
from python.util.round_numeric_columns import round_numeric_columns

GLOBAL_CUT_OFF_DATE = date(2019, 12, 31)


@transform_df(
    Output("/BP/IST-IG-DD/technical/dashboard_objects/pnl/PNL050_Approvals_Dataset"),
    input_df=Input("/BP/IST-IG-DD/data/published/all/pnl/history/PNL050_By_Portfolio_Adjusted"),
    ref_data_bench=Input("/BP/IST-IG-UC-User-Data/data/referencedata/Object_Bench_Filter")
)
def my_compute_function(input_df, ref_data_bench):
    df = input_df.filter(
        (input_df.Bench.isNotNull()) &
        (input_df.Valuation_Date > GLOBAL_CUT_OFF_DATE)
    )

    df_sorted = df.select(
        df.Valuation_Date,
        df.Bench,
        df.DTD_Total_And_Adj_USD,
        df.YTD_Total_And_Adj_USD,
        df.MTD_Total_And_Adj_USD
    )
    df_sorted = df_sorted.na.fill(0) #can't sum null values

    output_df = (df_sorted.groupBy(df_sorted.Valuation_Date, df_sorted.Bench)
        .agg(f.sum('YTD_Total_And_Adj_USD').alias('YTD'),
             f.sum('DTD_Total_And_Adj_USD').alias('DTD'),
             f.sum('MTD_Total_And_Adj_USD').alias('MTD'))
        .orderBy(['Valuation_Date', 'Bench'], ascending=[0, 1])
    )

    round_cols = [
        'YTD',
        'DTD',
        'MTD'
    ]
    output_df = round_numeric_columns(output_df, round_cols)
    
    output_df = (output_df
        .withColumn('CR_Checked', f.lit(False))
        .withColumn('Trader_Approval', f.lit(False))
        .withColumn('Commentary_Summary', f.lit(""))
        .withColumn('Trader_Notes', f.lit(""))
        .withColumn('PK_Bench', f.concat(output_df.Bench, f.lit(' '), output_df.Valuation_Date))
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
    return output_df
