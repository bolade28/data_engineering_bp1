# ===== import: python function
from pyspark.sql import functions as f
from datetime import date

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output

# ===== import: our functions


def aggregate_sum(df):
    df_selected = df.select(
        df['Valuation_Date'],
        df['Bench'],
        df['Team'],
        df['Strategy'],
        df['Trader'],
        df['Portfolio'],
        df['Grid_Point_Month'],
        df['Bucket_Name'],
        df['Risk_Factor_Name'],
        df['MR_Delta_MWh'],
        df['MR_Delta_MMBtu'],
        df['MR_Delta_Therms'],
    )
    df_grouped = df_selected.groupby(
        'Valuation_Date',
        'Bench',
        'Team',
        'Strategy',
        'Trader',
        'Portfolio',
        'Grid_Point_Month',
        'Bucket_Name',
        'Risk_Factor_Name'
    ).agg(
        f.sum('MR_Delta_MWh').alias('MR_Delta_MWh'),
        f.sum('MR_Delta_MMBtu').alias('MR_Delta_MMBtu'),
        f.sum('MR_Delta_Therms').alias('MR_Delta_Therms')
    ).orderBy(
        'Valuation_Date', ascending=0
    )
    return df_grouped


@transform_df(
    Output("/BP/IST-IG-DD/technical/dashboard_objects/pnl/EXPO130_Aggregation"),
    df_input=Input("/BP/IST-IG-DD/data/published/all/exposure/history/EXPO130_Endur_Monthly_Summary"),
)
def my_compute_function(df_input):
    df = df_input.filter(
        ( # not MR_Delta_UOM in ('BBL','GAL','EUR','GBP')
            (df_input['MR_Delta_UOM'] != 'BBL') &
            (df_input['MR_Delta_UOM'] != 'GAL') &
            (df_input['MR_Delta_UOM'] != 'EUR') &
            (df_input['MR_Delta_UOM'] != 'GBP')
        ) &
        (df_input['Risk_Factor_Name'].isNotNull()) &
        ( # not Risk_Factor_Name in ('DummyRF','FX_EUR','FX_GBP','Unknown')
            (df_input['Risk_Factor_Name'] != 'DummyRF') &
            (df_input['Risk_Factor_Name'] != 'FX_EUR') &
            (df_input['Risk_Factor_Name'] != 'FX_GBP') &
            (df_input['Risk_Factor_Name'] != 'Unknown')
        ) &
        (
            (
                df_input['Bench'] != 'European Gas'
            ) |
            ( # not Risk_Factor_Name in ('EMISSIONS','GERMAN POWER','SPAIN POWER','BRENT')
                (df_input['Risk_Factor_Name'] != 'EMISSIONS') &
                (df_input['Risk_Factor_Name'] != 'GERMAN POWER') &
                (df_input['Risk_Factor_Name'] != 'SPAIN POWER') &
                (df_input['Risk_Factor_Name'] != 'BRENT')
            )
        )

    )
    df_output = aggregate_sum(df)
    return df_output
