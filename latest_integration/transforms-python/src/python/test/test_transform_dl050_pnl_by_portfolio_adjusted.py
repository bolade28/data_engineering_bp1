from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl050_pnl_by_portfolio_adjusted import dtd_calc
from python.pnl.schema.schema_dl050_pnl_by_portfolio_adjusted import typed_output_schema
from python.pnl.schema.schema_dl050_pnl_by_portfolio_adjusted import transformed_output_schema


def test_transform_dnl101_pnl_detail(spark_session):
    input_data = [(
                  None, date(2019, 8, 19), "GTE_UK_BPGM", "GBP", 127235.15313413, 0.0, 127235.15313413, 830063.09287353,
                  0.0, 830063.09287353, 7474398.58360506, 0.0, 7474398.58360506),
                  (None, date(2019, 8, 19), "GTE_UK_DES", "GBP", -0.0, 0.0, -0.0, -0.0, 0.0, -0.0, -1.0E-8, 0.0,
                   -1.0E-8),
                  (None, date(2019, 8, 19), "GTE_EU_NOI_VIKD1Trading", "EUR", 7959.19310428, 0.0, 7959.19310428,
                   -38440.00638843, 0.0, -38440.00638843, -232690.71558766, 0.0, -232690.71558766),
                  ]

    expected_data = [(date(2019, 8, 19), "GTE_UK_BPGM", "GBP", "127235.15313413", 0.0, 127235.15313413, 830063.09287353,
                      0.0, 830063.09287353, 7474398.58360506, 0.0, 7474398.58360506, None),
                     (date(2019, 8, 19), "GTE_UK_DES", "GBP", "-0.0", 0.0, -0.0, -0.0, 0.0, -0.0, -1e-08, 0.0, -1e-08,
                      None),

                     (date(2019, 8, 19), "GTE_EU_NOI_VIKD1Trading", "EUR", "7959.19310428", 0.0, 7959.19310428, -
                      38440.00638843, 0.0, -38440.00638843, -232690.71558766, 0.0, -232690.71558766, None)
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
