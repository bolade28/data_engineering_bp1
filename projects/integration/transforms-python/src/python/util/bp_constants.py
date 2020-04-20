# ===== import: python function
from datetime import date

# ===== import: palantir functions

# ===== import: our functions


HIST_DATETIME_COL = 'Hist_Datetime'
VALUATION_DATE_COL = 'Valuation_Date'
SOURCE_COL = 'Source'

# date config constants
LOOK_BACK_PERIOD_LATEST = 2
LOOK_BACK_PERIOD_RECENT = 12
LOOK_BACK_PERIOD_CONFIG = 3
LOOK_AHEAD_YEARS = 3

# Flushing constant, set to True when FLUSHing
FLUSH = True
FLUSH_START_DATE = date(2019, 1, 2)

VERSION_NUMBER = 100000003

# schema constants
MAP_COLUMN_FORMAT = {"IntegerType": "int",
                     "DoubleType": "double",
                     "FloatType": "float"}
DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"