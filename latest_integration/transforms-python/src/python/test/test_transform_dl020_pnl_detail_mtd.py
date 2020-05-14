from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl020_pnl_detail_mtd import dtd_calc
from python.pnl.schema.schema_dl020_pnl_detail_mtd import typed_output_schema
from python.pnl.schema.schema_dl020_pnl_detail_mtd import transformed_output_schema


def test_transform_dl020(spark_session):

    input_data = [(None, 3531621, "USD", 0, "ENGY-SWAP", "LNG_LON_PHYS_HUB_PH", 0, date(2019, 3, 11), "EOD", 1, 0.0,
                   -1182.62000246, -1182., 0, None, 0, "USD", 1, 1, 5, 0.94440864, date(2021, 6, 30), "Profile", 5, 5,
                   2.6, -1, 0, 0.0, date(2021, 6, 3), None, 0, 0.0, "Cash Settlement", date(2021, 6, 1), 0.0,
                   -1182.62000246, 3531622, "Validated", -1182.62000246, -450000.0),

                  (None, 2070202, "EUR", 0, "COMM-FEE", "GTE_EU_SWAP_HEDGES", 0, date(2019, 3, 11), "EOD", 1, 0.0, 0.0,
                   0., 0, "Brokerage Refund", 0, "EUR", 0, 0, 0, 1.0, date(2015, 1, 31), "PhysCash", 1, 2, 34.56, -1, 0,
                   0.0, date(2015, 2, 19), date(2015, 2, 19), 0, 0.0, "Cash Settlement", date(2015, 1, 1), 0.0, 0.0,
                   2141722, "Validated", 0.0, 1.0),

                  (None, 3413441, "GBP", 0, "ENGY-EXCH-FUT", "GTE_UK_NBP3", 0, date(2019, 3, 11), "EOD", 1, 0.0, 0.0,
                   0., 20002, "Broker Fee", 0, "GBP", 0, 0, 0, 1.0, date(2018, 8, 17), "PhysCash", 2, 2, 0.35567175,
                   -1, 0, 0.0, date(2018, 8, 17), None, 1, 0.0, "Cash Settlement", date(2018, 8, 17), 0.0, 0.0,
                   3413441, "Validated", 0.0, -5.0),
                  ]

    expected_data = [(date(2019, 3, 11), 3531621, "Validated", "LNG_LON_PHYS_HUB_PH", "ENGY-SWAP", date(2021, 6, 1),
                      date(2021, 6, 30), 0.0, -1182.0, -1182.62000246, 0.0, -1182.62000246, -1182.62000246, 0.0, None,
                      "USD", 0, 0, "EOD", 1, 0, None, 0, "USD", 0.94440864, "Profile", 5, 5, 2.6, -1, 0,
                      date(2021, 6, 3), None, 0, "Cash Settlement", 0.0, 3531622, -450000.0, 1, 1, 5),

                     (date(2019, 3, 11), 2070202, "Validated", "GTE_EU_SWAP_HEDGES", "COMM-FEE", date(2015, 1, 1),
                      date(2015, 1, 31), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None,
                      "EUR", 0, 0, "EOD", 1, 0, "Brokerage Refund", 0, "EUR", 1.0, "PhysCash", 1, 2, 34.56, -1, 0,
                      date(2015, 2, 19), date(2015, 2, 19), 0, "Cash Settlement", 0.0, 2141722, 1.0, 0, 0, 0),

                     (date(2019, 3, 11), 3413441, "Validated", "GTE_UK_NBP3", "ENGY-EXCH-FUT", date(2018, 8, 17),
                      date(2018, 8, 17), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None,
                      "GBP", 0, 0, "EOD", 1, 20002, "Broker Fee", 0, "GBP", 1.0, "PhysCash", 2, 2, 0.35567175, -1, 0,
                      date(2018, 8, 17), None, 1, "Cash Settlement", 0.0, 3413441, -5.0, 0, 0, 0),
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())