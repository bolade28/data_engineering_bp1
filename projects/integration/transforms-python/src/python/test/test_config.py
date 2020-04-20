from datetime import date
from python.util.bp_config import Config
from python.util.bp_constants import LOOK_BACK_PERIOD_CONFIG


def test_current(spark_session):
    current_date = date(2019, 10, 10)
    look_back_period = 2
    look_ahead_years = 3
    custom_flush_start_date = date(2020, 1, 2)

    # These are the expected results for a "standard" flush given what is in bp_constants.py,
    # given the above look-ahead period of 3 years.
    # The test will fail if FLUSH_START_DATE in that file changes.
    expected_flush_data = [
        date(2019, 1, 2),
        date(2022, 1, 2)
    ]

    # These are the expected results for a "custom" flush where the start date is specified,
    # given the above look-ahead period of 3 years.
    expected_custom_flush_data = [
        date(2020, 1, 2),
        date(2023, 1, 2)
    ]

    expected_non_flush_data = [
        date(2019, 10, 8),
        date(2019, 10, 10)
    ]

    # flush
    flush_value = True
    config = Config(current_date=current_date,
                    look_ahead_years=look_ahead_years,
                    look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=flush_value)
    assert(config.start_date == expected_flush_data[0])
    assert(config.end_date == expected_flush_data[1])

    # custom flush
    flush_value = True
    config = Config(current_date=current_date,
                    look_ahead_years=look_ahead_years,
                    look_back_period=LOOK_BACK_PERIOD_CONFIG,
                    flush=flush_value,
                    flush_start_date=custom_flush_start_date)
    assert(config.start_date == expected_custom_flush_data[0])
    assert(config.end_date == expected_custom_flush_data[1])

    # non-flush
    flush_value = False
    config = Config(current_date=current_date,
                    look_back_period=look_back_period,
                    look_ahead_years=look_ahead_years,
                    flush=flush_value)
    assert(config.start_date == expected_non_flush_data[0])
    assert(config.end_date == expected_non_flush_data[1])


def test_recent_flush(spark_session):
    current_date = date(2019, 10, 10)
    look_back_period = 12
    look_ahead_years = 3

    expected_data = [
        date(2019, 9, 28),
        date(2019, 10, 10)
    ]

    flush = True
    config = Config(current_date=current_date,
                    look_back_period=look_back_period,
                    look_ahead_years=look_ahead_years,
                    flush=flush)
    assert(config.start_date == expected_data[0])
    assert(config.end_date == expected_data[1])

    flush = False
    config = Config(current_date=current_date,
                    look_back_period=look_back_period,
                    look_ahead_years=look_ahead_years,
                    flush=flush)
    assert(config.start_date == expected_data[0])
    assert(config.end_date == expected_data[1])


def test_recent_edge(spark_session):
    current_date = date(2019, 1, 1)
    look_back_period = 12
    look_ahead_years = 3

    expected_data = [
            date(2018, 12, 20),
            date(2019, 1, 1)
        ]

    config = Config(current_date=current_date,
                    look_back_period=look_back_period,
                    look_ahead_years=look_ahead_years)
    assert(config.start_date == expected_data[0])
    assert(config.end_date == expected_data[1])


def test_latest(spark_session):
    current_date = date(2019, 10, 10)
    look_back_period = 2
    look_ahead_years = 3

    expected_data = [
            date(2019, 10, 8),
            date(2019, 10, 10)
        ]

    config = Config(current_date=current_date,
                    look_back_period=look_back_period,
                    look_ahead_years=look_ahead_years)
    assert(config.start_date == expected_data[0])
    assert(config.end_date == expected_data[1])
