from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl075_pnl_explained_by_portfolio_and_instrument import dtd_calc
from python.pnl.schema.schema_dl075_pnl_explained_by_portfolio_and_instrument import typed_output_schema
from python.pnl.schema.schema_dl075_pnl_explained_by_portfolio_and_instrument import transformed_output_schema


def test_transform_dl075(spark_session):
    input_data = [(None, date(2019, 1, 4), "GTE_UK_BPGM", "GBP", "COMM-FEE", -8075.19942618, -8075.19942618,
                   0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                   "No", 0.0, 0.0, 0.0, 0.0),

                  (None, date(2019, 1, 4), "GTE_UK_BPGM", "GBP", "GAS-PHYS", 103703.21372205, 253.002871,
                   -377711.15041688, 0.0, 0.0, 0.0, 489083.72478565, -2066.96476768, 0.0, -
                   5853.01150121, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                   "No", 0.0, 0.0, 0.0, -2.38724884),

                  (None, date(2019, 1, 4), "GTE_UK_BPGM", "GBP", "GAS-IMB", 27763.2421709, -15692.24878977,
                   35180.26754853, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8275.22341214, 0.0, 0.0, 0.0,
                   0.0, 0.0, 0.0, 0.0, "No", 0.0, 0.0, 0.0, 0.0),
                  ]

    expected_data = [(date(2019, 1, 4), "GTE_UK_BPGM", "GBP", "COMM-FEE", -8075.19942618, -8075.19942618,
                      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                      0.0, "No", 0.0, 0.0, 0.0, 0.0, None),

                     (date(2019, 1, 4), "GTE_UK_BPGM", "GBP", "GAS-PHYS", 103703.21372205, 253.002871, -377711.15041688,
                      0.0, 0.0, 0.0, 489083.72478565, -2066.96476768, 0.0, -
                      5853.01150121, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                      "No", 0.0, 0.0, 0.0, -2.38724884, None),

                     (date(2019, 1, 4), "GTE_UK_BPGM", "GBP", "GAS-IMB", 27763.2421709, -15692.24878977,
                      35180.26754853, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8275.22341214, 0.0, 0.0,
                      0.0, 0.0, 0.0, 0.0, 0.0, "No", 0.0, 0.0, 0.0, 0.0, None),
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
