from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl030_pnl_detail_ytd import dtd_calc
from python.pnl.schema.schema_dl030_pnl_detail_ytd import typed_output_schema
from python.pnl.schema.schema_dl030_pnl_detail_ytd import transformed_output_schema


def test_transform_dnl101_pnl_detail(spark_session):

    input_data = [(None, 2952202, "EUR", 0, "GAS-IMB", "BP_IT_Balancing", 0, date(2019, 1, 31), "EOD", 1, 0.0, 0.0,
                   0.0, 0, None, 0, None, 3, 3, 256, 1.0, date(2018, 6, 14), "Profile", 256, 256, 0.0, -1, 0, 0.0,
                   date(2018, 7, 20), date(2018, 6, 14), 1, 0.0, "Physical Settlement", date(2018, 6, 14), 0.0, 0.0,
                   3459889, "Validated", 0.0, 0.0),

                  (None, 2952202, "EUR", 0, "GAS-IMB", "BP_IT_Balancing", 0, date(2019, 1, 31), "EOD", 1, 0.0, 0.0,
                   0.0, 0, None, 0, None, 3, 3, 263, 1.0, date(2018, 6, 21), "Profile", 263, 263, 0.0, -1, 0, 0.0,
                   date(2018, 7, 20), date(2018, 6, 21), 1, 0.0, "Physical Settlement", date(2018, 6, 21), 0.0, 0.0,
                   3459889, "Validated", 0.0, 0.0),

                  (None, 2952202, "EUR", 0, "GAS-IMB", "BP_IT_Balancing", 0, date(2019, 1, 31), "EOD", 1, 0.0, 0.0, 0.0,
                   0, None, 0, None, 3, 3, 264, 1.0, date(2018, 6, 22), "Profile", 264, 264, 0.0, -1, 0, 0.0,
                   date(2018, 7, 20), date(2018, 6, 22), 1, 0.0, "Physical Settlement", date(2018, 6, 22), 0.0, 0.0,
                   3459889, "Validated", 0.0, 0.0),
                  ]

    expected_data = [(date(2019, 1, 31), 2952202, "Validated", "BP_IT_Balancing", "GAS-IMB", date(2018, 6, 14),
                      date(2018, 6, 14), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None, "EUR", 0, 0, "EOD", 1, 0, None, 0,
                      None, 1.0, "Profile", 256, 256, 0.0, -1, 0, date(2018, 7, 20), date(2018, 6, 14), 1,
                      "Physical Settlement", 0.0, 3459889, 0.0, 3, 3, 256),

                     (date(2019, 1, 31), 2952202, "Validated", "BP_IT_Balancing", "GAS-IMB", date(2018, 6, 21),
                      date(2018, 6, 21), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None, "EUR", 0, 0, "EOD", 1, 0, None, 0,
                      None, 1.0, "Profile", 263, 263, 0.0, -1, 0, date(2018, 7, 20), date(2018, 6, 21), 1,
                      "Physical Settlement", 0.0, 3459889, 0.0, 3, 3, 263),

                     (date(2019, 1, 31), 2952202, "Validated", "BP_IT_Balancing", "GAS-IMB", date(2018, 6, 22),
                      date(2018, 6, 22), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None, "EUR", 0, 0, "EOD", 1, 0, None, 0,
                      None, 1.0, "Profile", 264, 264, 0.0, -1, 0, date(2018, 7, 20), date(2018, 6, 22), 1,
                      "Physical Settlement", 0.0, 3459889, 0.0, 3, 3, 264),
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
