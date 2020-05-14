# ===== import: python function
from datetime import date, timedelta

# ===== import: palantir functions

# ===== import: our functions
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG, LOOK_AHEAD_YEARS, FLUSH_START_DATE, FLUSH

# ===== import: logging
import logging
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, *args, **kwargs):
        '''
        This function:
            decides the values for start_date and end_date which will be used for filtering
            There are 3 cases which it is used: in current, view/latest, view/recent
                Only current is affected by FLUSH
                    If flush, the start and end dates would be drawn from bp_constants
                    unless overridden by the flush_start_date parameter
                    If not flush, the operation is the same as the other 2 (latest and recent)
                The other 2 (latest and recent), which are snapshot, are not affected by FLUSH
                    They take a date range depending on the look_back_period

        Args:
            params1: args
                empty
            params2: kwargs
                look_back_period (needed)
                current_date (used in test)
                flush (used in test)
                look_ahead_years (used in test)

        '''

        current_date = date.today()
        look_back_period = None
        flush_variable = FLUSH
        look_ahead_years = LOOK_AHEAD_YEARS
        flush_start_date_variable = FLUSH_START_DATE

        for key, value in kwargs.items():
            if key == "look_back_period":
                look_back_period = value
                continue
            if key == "current_date":
                current_date = value
                continue
            if key == "flush":
                flush_variable = value
                continue
            if key == "look_ahead_years":
                look_ahead_years = value
                continue
            if key == "flush_start_date":
                flush_start_date_variable = value
                continue

        if look_back_period == LOOK_BACK_PERIOD_CONFIG and flush_variable:
            self.start_date = flush_start_date_variable
            self.end_date = flush_start_date_variable.replace(year=flush_start_date_variable.year + look_ahead_years)
        else:
            self.start_date = current_date - timedelta(days=look_back_period)
            self.end_date = current_date

        logger.info('=========================== start_date = %s, end_date = %s', self.start_date, self.end_date)


class Config_exposure:
    def __init__(self, look_back_period, current_date):
        self.start_date = date(2019, 6, 26)
        self.end_date = date(2020, 10, 10)
    '''
    # need to include df in parameters
    def __init__(self, df, look_back_period, current_date):
        df_filtered = self._filter_run_datetime(current_date, look_back_period, df)
        date_range = self._get_min_max_date(df_filtered, 'Valuation_Date')
        self.start_date = date_range[0]
        self.end_date = date_range[1]

    def _filter_run_datetime(self, current_date, look_back_period, df):
        date_threshold = current_date - timedelta(days=look_back_period)
        df = df.withColumn("Run_Datetime_Date", F.to_date(F.col("Run_Datetime")))
        df_filtered = df.filter(df.Run_Datetime_Date >= date_threshold)
        return df_filtered

    # get max and min dates for a given column
    def _get_min_max_date(self, df, columnName):

        date_list = df.select(columnName).collect()
        date_max = max(date_list)[columnName]
        date_min = min(date_list)[columnName]

        return [date_min, date_max]
    '''
