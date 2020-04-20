# ===== import: python function
import pyspark.sql.functions as f

# ===== import: palantir functions

# ===== import: our functions


def add_grid_point_month(df):
    df = df.withColumn('Grid_Point_Month',
        f.trunc('Grid_Point_Start_Date', 'month')
    )
    return df