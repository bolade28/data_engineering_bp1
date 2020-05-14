from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl010_pnl_detail import dtd_calc
from python.pnl.schema.schema_dl010_pnl_detail import typed_output_schema
from python.pnl.schema.schema_dl010_pnl_detail import transformed_output_schema


def test_transform_dl010_pnl_detail(spark_session):
    input_data = [(None, 2193134, 20201, 0, 0, 'EUR', 0, 'ENGY-EXCH-FUT', 'GEP_EU_SPEC', 0,
                   date(2019, 7, 15), 'EOD', 1, 0.0, 1325250.0, 1325250.0, 0, 'Interest', 11700.0, 0, 'EUR', 1.0,
                   date(2020, 12, 31), 'Profile', 362343, 0, 0, 0, 29.45, -1, 0, 0, 1325250.0, date(2020, 12, 14),
                   date(2020, 12, 14), 2, 0.0, 'Physical Settlement', date(2020, 12, 1), 7.05, 1325250.0, 2193134,
                   'Validated', 1325250.0, 45000.0, 0.0, 1313550.0, 1313550.0, 1313550.0, 0.0, 1313550.0, 3, 1313550.0),

                  (None, 2194000, 20201, 0, 0, 'EUR', 0, 'ENGY-EXCH-FUT', 'GEP_EU_SPEC', 0,
                   date(2019, 7, 15), 'EOD', 1, 0.0, 0.0, 0.0, 2, 'Broker Fee', 0.0, 0, 'EUR', 1.0,
                   date(2014, 2, 28), 'PhysCash', 362343, 0, 0, 0, 0.0, -1, 0, 0, 0.0, date(2014, 2, 28),
                   None, 1, 0.0, 'Cash Settlement', date(2014, 2, 28), 0.0, 0.0, 2194000,
                   'Validated', 0.0, -500.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3, 0.0),

                  (None, 2194043, 20203, 0, 0, 'EUR', 0, 'ENGY-EXCH-FUT', 'GEP_EU_SPEC_ILLIQUID',
                   0, date(2019, 7, 15), 'EOD', 1, 0.0, 7391950.0, 7391950.0, 0, 'Interest', 65260.0, 0, 'EUR', 1.0,
                   date(2020, 12, 31), 'Profile', 362343, 0, 0, 0, 29.45, -1, 0, 0, 7391950.0, date(2020, 12, 14),
                   date(2020, 12, 14), 2, 0.0, 'Physical Settlement', date(2020, 12, 1), 8.34, 7391950.0, 2194043,
                   'Validated', 7391950.0, 251000.0, 0.0, 7326690.0, 7326690.0, 7326690.0, 0.0, 7326690.0, 3, 7326690.0)
                  ]

    expected_data = [(date(2019, 7, 15), 2193134, "Validated", "GEP_EU_SPEC", "ENGY-EXCH-FUT", date(2020, 12, 1),
                      date(2020, 12, 31), 0.0, 1325250.0, 1325250.0, 0.0, 1325250.0, 1325250.0, 0.0, 1313550.0,
                      1313550.0, 0.0, 1313550.0, 1313550.0, 0.0, 11700.0, 11700.0, 0.0, 11700.0, 11700.0, 1325250.0,
                      1313550.0, 3, None, 20201, 0, 0, "EUR", 0, 0, "EOD", 1, 0, "Interest", 0, "EUR", 1.0, "Profile",
                      362343, 0, 0, 0, 29.45, -1, 0, 0, date(2020, 12, 14), date(2020, 12, 14), 2,
                      "Physical Settlement", 7.05, 2193134, 45000.0),

                     (date(2019, 7, 15), 2194000, "Validated", "GEP_EU_SPEC", "ENGY-EXCH-FUT", date(2014, 2, 28),
                      date(2014, 2, 28), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                      0.0, 0.0, 0.0, 0.0, 3, None, 20201, 0, 0, "EUR", 0, 0, "EOD", 1, 2, "Broker Fee", 0, "EUR", 1.0,
                      "PhysCash", 362343, 0, 0, 0, 0.0, -1, 0, 0, date(2014, 2, 28), None, 1, "Cash Settlement", 0.0,
                      2194000, -500.0),

                     (date(2019, 7, 15), 2194043, "Validated", "GEP_EU_SPEC_ILLIQUID", "ENGY-EXCH-FUT",
                      date(2020, 12, 1), date(2020, 12, 31), 0.0, 7391950.0, 7391950.0, 0.0, 7391950.0, 7391950.0, 0.0,
                      7326690.0, 7326690.0, 0.0, 7326690.0, 7326690.0, 0.0, 65260.0, 65260.0, 0.0, 65260.0, 65260.0,
                      7391950.0, 7326690.0, 3, None, 20203, 0, 0, "EUR", 0, 0, "EOD", 1, 0, "Interest", 0, "EUR", 1.0,
                      "Profile", 362343, 0, 0, 0, 29.45, -1, 0, 0, date(2020, 12, 14), date(2020, 12, 14), 2,
                      "Physical Settlement", 8.34, 2194043, 251000.0)
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
