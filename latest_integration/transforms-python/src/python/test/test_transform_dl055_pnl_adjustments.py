from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl055_pnl_adjustments import dtd_calc
from python.pnl.schema.schema_dl055_pnl_adjustments import typed_output_schema
from python.pnl.schema.schema_dl055_pnl_adjustments import transformed_output_schema


def test_transform_dnl101_pnl_detail(spark_session):

    input_data = [(None, date(2019, 8, 29), "LNG_LON_FIN1_GRAIN", 0,
                   "Reversal: Adjustment: 26c update to price on 3531537", date(2019, 1, 2), date(2019, 12, 31), "USD",
                   0.0, 0.0, -841787.0, "USD", 0.0, 0.0, -841787.0),

                  (None, date(2019, 8, 29), "BP_IT_LNG", 0, "Adjustment: BP_IT_LNG", date(2019, 1, 14),
                   date(2019, 12, 31), "EUR", 0.0, 0.0, 249092.0, "EUR", 0.0, 0.0, 249092.0),

                  (None, date(2019, 8, 29), "BP_IT_LNG", 0,
                   "Adjustment: Endur update with 2019 actual operations activity and costs", date(2019, 6, 30),
                   date(2019, 12, 31), "EUR", 0.0, 0.0, -279092.0, "EUR", 0.0, 0.0, -279092.0),
                  ]

    expected_data = [(date(2019, 8, 29), "LNG_LON_FIN1_GRAIN", 0,
                      "Reversal: Adjustment: 26c update to price on 3531537", date(
                          2019, 1, 2), date(2019, 12, 31), "USD", 0.0, 0.0,
                      -841787.0, "USD", 0.0, 0.0, -841787.0, None),

                     (date(2019, 8, 29), "BP_IT_LNG", 0, "Adjustment: BP_IT_LNG", date(2019, 1, 14), date(2019, 12, 31),
                      "EUR", 0.0, 0.0, 249092.0, "EUR", 0.0, 0.0, 249092.0, None),

                     (date(2019, 8, 29), "BP_IT_LNG", 0,
                      "Adjustment: Endur update with 2019 actual operations activity and costs", date(2019, 6, 30),
                      date(2019, 12, 31), "EUR", 0.0, 0.0, -279092.0, "EUR", 0.0, 0.0, -279092.0, None),
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
