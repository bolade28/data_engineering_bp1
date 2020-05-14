from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl120_market_risk_stage_result import dtd_calc
from python.pnl.schema.schema_dl120_market_risk_stage_result import typed_output_schema
from python.pnl.schema.schema_dl120_market_risk_stage_result import transformed_output_schema


def test_transform_dl120(spark_session):
    input_data = [(None, 3100, date(2019, 9, 27), "S", 3705847, "LNG_LON_PHYS-BU", "haaf8h",
                   "LNG_LON_PHYS_NBP_PH", "LNG_LON_FIN-BU", "LNG_LON_PHYS-BU", "GBP", "GAS-PHYS", date(2019, 2, 26),
                   None, "Commodity", "LNG_LON_FIN1_GRAIN", "None", "None", 3705846, "",
                   "GAS_OTC_NBP", "Therms", 29, "28-Sep-19/fwd-d", date(2019,
                                                                        9, 28), date(2019, 9, 28), date(2019, 9, 28),
                   date(2019, 9, 28), 10.0, 0.0, 0.0, 0.0, "NBP", 0.0, 100.0, "GBP", 1.0, "Th(15)", 1.0, 10000.0, 0.0,
                   0.0, 0.0, 2520.0, "Nr Weekend", 26, 1, date(2019, 9, 28), date(2019, 9, 29), "GAS", "BASE"),

                  (None, 3100, date(2019, 9, 27), "S", 3705847, "LNG_LON_PHYS-BU", "haaf8h",
                   "LNG_LON_PHYS_NBP_PH", "LNG_LON_FIN-BU", "LNG_LON_PHYS-BU", "GBP", "GAS-PHYS", date(2019, 2, 26),
                   None, "Commodity", "LNG_LON_FIN1_GRAIN", "None", "None", 3705846, "",
                   "GAS_OTC_NBP", "Therms", 30, "29-Sep-19/fwd-d", date(2019,
                                                                        9, 29), date(2019, 9, 29), date(2019, 9, 29),
                   date(2019, 9, 29), 10.0, 0.0, 0.0, 0.0, "NBP", 0.0, 100.0, "GBP", 1.0, "Th(15)", 1.0, 10000.0, 0.0,
                   0.0, 0.0, 2520.0, "Nr Weekend", 26, 1, date(2019, 9, 28), date(2019, 9, 29), "GAS", "BASE"),

                  (None, 3100, date(2019, 9, 27), "S", 3705847, "LNG_LON_PHYS-BU", "haaf8h",
                   "LNG_LON_PHYS_NBP_PH", "LNG_LON_FIN-BU", "LNG_LON_PHYS-BU", "GBP", "GAS-PHYS", date(2019, 2, 26),
                   None, "Commodity", "LNG_LON_FIN1_GRAIN", "None", "None", 3705846, "",
                   "GAS_OTC_NBP", "Therms", 31, "Sep-19/fwd-d", date(2019, 9, 30), date(2019, 9, 30), date(2019, 9, 30),
                   date(2019, 9, 30), 10.0, 0.0, 0.0, -6.97, "NBP", 0.0, 100.0, "GBP", 1.0, "Th(15)", 1.0, 10000.0, 0.0,
                   0.0, -6.97, 2550.0, "D+1", 25, 2, date(2019, 9, 30), date(2019, 9, 30), "GAS", "BASE"),
                  ]

    expected_data = [(3100, date(2019, 9, 27), "S", 3705847, "LNG_LON_PHYS-BU", "haaf8h",
                      "LNG_LON_PHYS_NBP_PH", "LNG_LON_FIN-BU", "LNG_LON_PHYS-BU", "GBP",
                      "GAS-PHYS", date(2019, 2, 26), None, "Commodity", "LNG_LON_FIN1_GRAIN",
                      "None", "None", 3705846, "", "GAS_OTC_NBP", "Therms", 29, "28-Sep-19/fwd-d",
                      date(2019, 9, 28), date(2019, 9, 28), date(2019, 9, 28), date(2019, 9, 28), 10.0, 0.0, 0.0, 0.0,
                      "NBP", 0.0, 100.0, "GBP", 1.0, "Th(15)", 1.0, 10000.0, 0.0, 0.0, 0.0, 2520.0, "Nr Weekend", 26, 1,
                      date(2019, 9, 28), date(2019, 9, 29), "GAS", "BASE", None),

                     (3100, date(2019, 9, 27), "S", 3705847, "LNG_LON_PHYS-BU", "haaf8h",
                         "LNG_LON_PHYS_NBP_PH", "LNG_LON_FIN-BU", "LNG_LON_PHYS-BU", "GBP",
                         "GAS-PHYS", date(2019, 2, 26), None, "Commodity", "LNG_LON_FIN1_GRAIN",
                         "None", "None", 3705846, "", "GAS_OTC_NBP", "Therms", 30, "29-Sep-19/fwd-d",
                         date(2019, 9, 29), date(2019, 9, 29), date(
                             2019, 9, 29), date(2019, 9, 29), 10.0, 0.0, 0.0, 0.0,
                         "NBP", 0.0, 100.0, "GBP", 1.0, "Th(15)", 1.0, 10000.0, 0.0, 0.0, 0.0, 2520.0, "Nr Weekend", 26,
                         1, date(2019, 9, 28), date(2019, 9, 29), "GAS", "BASE", None),

                     (3100, date(2019, 9, 27), "S", 3705847, "LNG_LON_PHYS-BU", "haaf8h",
                         "LNG_LON_PHYS_NBP_PH", "LNG_LON_FIN-BU", "LNG_LON_PHYS-BU", "GBP",
                         "GAS-PHYS", date(2019, 2, 26), None, "Commodity", "LNG_LON_FIN1_GRAIN",
                         "None", "None", 3705846, "", "GAS_OTC_NBP", "Therms", 31, "Sep-19/fwd-d",
                         date(2019, 9, 30), date(2019, 9, 30), date(
                             2019, 9, 30), date(2019, 9, 30), 10.0, 0.0, 0.0, -6.97,
                         "NBP", 0.0, 100.0, "GBP", 1.0, "Th(15)", 1.0, 10000.0, 0.0, 0.0, -6.97, 2550.0, "D+1", 25, 2,
                         date(2019, 9, 30), date(2019, 9, 30), "GAS", "BASE", None),
                     ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
