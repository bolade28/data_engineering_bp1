from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl125_market_risk_stage_result_emission import dtd_calc
from python.pnl.schema.schema_dl125_market_risk_stage_result_emission import typed_output_schema
from python.pnl.schema.schema_dl125_market_risk_stage_result_emission import transformed_output_schema


def test_transform_dl125(spark_session):
    input_data = [(None, 3101, date(2019, 9, 30), "S", 2208400, "GEP_BPGM_SPEC-BU", "ol_superuser",
                   "GEP_EU_SPEC", "NEWEDGE-BU", "GEP_BPGM_SPEC-BU", "EUR", "ENGY-EXCH-FUT", date(2014, 6, 10),
                   None, "ComFut", "", "None", "None", 0, "MIG_Aligne_AUE447_1", "EM_EUETS_EUA",
                   "tCO2e", 5, "Dec-19/fwd-m", date(2019, 12, 1), date(2019, 12,
                                                                       31), date(2019, 12, 1), date(2019, 12, 31),
                   "-250.00", 0.0, "0.00", "7.23", "EUA", 0.01, 1.0, "EUR", 1.0, "tCO2e", 1.0, -25000.00000001, 0.0,
                   0.0, 7.23, -618000.00000014, 201912, 0, 0, date(2019, 12, 1), date(2019, 12, 31), "EMISSION"),

                  (None, 3101, date(2019, 9, 30), "S", 2208620, "GEP_BPGM_SPEC-BU", "ol_superuser",
                   "GEP_EU_SPEC", "NEWEDGE-BU", "GEP_BPGM_SPEC-BU", "EUR", "ENGY-EXCH-FUT", date(2014, 6, 10),
                   None, "ComFut", "", "None", "None", 0, "MIG_Aligne_AUE446_1", "EM_EUETS_EUA",
                   "tCO2e", 5, "Dec-19/fwd-m", date(2019, 12, 1), date(2019, 12,
                                                                       31), date(2019, 12, 1), date(2019, 12, 31),
                   "-250.00", 0.0, "0.00", "7.23", "EUA", 0.01, 1.0, "EUR", 1.0, "tCO2e", 1.0, -25000.00000001, 0.0,
                   0.0, 7.23, -618000.00000014, 201912, 0, 0, date(2019, 12, 1), date(2019, 12, 31), "EMISSION"),

                  (None, 3101, date(2019, 9, 30), "S", 2208688, "GEP_BPGM_SPEC-BU", "ol_superuser",
                   "GEP_EU_SPEC", "NEWEDGE-BU", "GEP_BPGM_SPEC-BU", "EUR", "ENGY-EXCH-FUT", date(2014, 6, 10),
                   None, "ComFut", "", "None", "None", 0, "MIG_Aligne_AUE445_1", "EM_EUETS_EUA",
                   "tCO2e", 5, "Dec-19/fwd-m", date(2019, 12, 1), date(2019, 12,
                                                                       31), date(2019, 12, 1), date(2019, 12, 31),
                   "-110.00", 0.0, "0.00", "3.18", "EUA", 0.01, 1.0, "EUR", 1.0, "tCO2e", 1.0, -11000.0, 0.0, 0.0,
                   3.18, -271920.00000007, 201912, 0, 0, date(2019, 12, 1), date(2019, 12, 31), "EMISSION"),
                  ]

    expected_data = [
        (3101, date(2019, 9, 30), "S", 2208400, "GEP_BPGM_SPEC-BU", "ol_superuser", "GEP_EU_SPEC", "NEWEDGE-BU",
         "GEP_BPGM_SPEC-BU", "EUR", "ENGY-EXCH-FUT", date(2014, 6, 10), None, "ComFut", "", "None", "None", 0,
         "MIG_Aligne_AUE447_1", "EM_EUETS_EUA", "tCO2e", 5, "Dec-19/fwd-m", date(2019, 12, 1), date(2019, 12, 31),
         date(2019, 12, 1), date(2019, 12, 31), "-250.00", 0.0, "0.00", "7.23", "EUA", 0.01, 1.0, "EUR", 1.0, "tCO2e",
         1.0, -25000.00000001, 0.0, 0.0, 7.23, -618000.00000014, 201912, 0, 0, date(2019, 12, 1), date(2019, 12, 31),
         "EMISSION", None),

        (3101, date(2019, 9, 30), "S", 2208620, "GEP_BPGM_SPEC-BU", "ol_superuser", "GEP_EU_SPEC", "NEWEDGE-BU",
         "GEP_BPGM_SPEC-BU", "EUR", "ENGY-EXCH-FUT", date(2014, 6, 10), None, "ComFut", "", "None", "None", 0,
         "MIG_Aligne_AUE446_1", "EM_EUETS_EUA", "tCO2e", 5, "Dec-19/fwd-m", date(2019, 12, 1), date(2019, 12, 31),
         date(2019, 12, 1), date(2019, 12, 31), "-250.00", 0.0, "0.00", "7.23", "EUA", 0.01, 1.0, "EUR", 1.0, "tCO2e",
         1.0, -25000.00000001, 0.0, 0.0, 7.23, -618000.00000014, 201912, 0, 0, date(2019, 12, 1), date(2019, 12, 31),
         "EMISSION", None),

        (3101, date(2019, 9, 30), "S", 2208688, "GEP_BPGM_SPEC-BU", "ol_superuser", "GEP_EU_SPEC", "NEWEDGE-BU",
         "GEP_BPGM_SPEC-BU", "EUR", "ENGY-EXCH-FUT", date(2014, 6, 10), None, "ComFut", "", "None", "None", 0,
         "MIG_Aligne_AUE445_1", "EM_EUETS_EUA", "tCO2e", 5, "Dec-19/fwd-m", date(2019, 12, 1), date(2019, 12, 31),
         date(2019, 12, 1), date(2019, 12, 31), "-110.00", 0.0, "0.00", "3.18", "EUA", 0.01, 1.0, "EUR", 1.0, "tCO2e",
         1.0, -11000.0, 0.0, 0.0, 3.18, -271920.00000007, 201912, 0, 0, date(2019, 12, 1), date(2019, 12, 31),
         "EMISSION", None),
    ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
