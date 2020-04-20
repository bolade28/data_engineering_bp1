# ===== import: python function
import datetime
import calendar
import pyspark.sql.functions as f
from pyspark.sql import Window

# ===== import: palantir functions

# ===== import: our functions

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


def filter_latest_runs(df, partitionCols, runTimeDateCol):
    logger.info("=================filter_latest_runs: STARTED")
    w = Window.partitionBy(partitionCols)

    df = df.withColumn('maxRT', f.max(runTimeDateCol).over(w)) \
        .where(f.col(runTimeDateCol) == f.col('maxRT')) \
        .drop('maxRT')
    logger.info("=================filter_latest_runs: FINISHED")
    return df


def get_first_day_of_month(dt):
    return datetime.date(dt.year, dt.month, 1)


def get_last_day_of_month(dt):
    bom = get_first_day_of_month(dt)
    days_in_month = calendar.monthrange(bom.year, bom.month)[1]
    return bom + datetime.timedelta(days=days_in_month-1)

# def get_last_day_of_month(dt):
#   days_in_month = calendar.monthrange(dt.year, dt.month)[1]
#   return datetime.date(dt.year, dt.month, days_in_month)


def get_quarter(dt):
    return int((dt.month-1) / 3) + 1


def monthly_buckets(start_date, end_date):
    retval = []
    curr_date = start_date
    while curr_date <= end_date:
        eom = get_last_day_of_month(curr_date)
        if eom > end_date:
            eom = end_date
        num_days = (eom - curr_date).days+1
        retval.append((curr_date, eom, num_days))
        curr_date = eom + datetime.timedelta(days=1)
    return retval
