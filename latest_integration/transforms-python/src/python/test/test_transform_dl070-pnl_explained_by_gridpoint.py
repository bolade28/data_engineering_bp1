from datetime import date
from pandas.testing import assert_frame_equal
from python.pnl.transformed.dl070_pnl_explained_by_gridpoint import dtd_calc
from python.pnl.schema.schema_dl070_pnl_explained_by_gridpoint import typed_output_schema
from python.pnl.schema.schema_dl070_pnl_explained_by_gridpoint import transformed_output_schema


def test_transform_dl070(spark_session):
    input_data = [(None, date(2019, 8, 13), "GTE_UK_BPGM", "IR_LIBOR.GBP", date(2019, 8, 17),
                   date(2019, 8, 20), -64217.1037641, 0.00693,
                   0.0069325, -0.1586031, 1.0E-8, -0.1586031, 1.0E-8, -0.19140222, 1.0E-8, -0.64217104, 3.3E-7, "'1w",
                   date(2019, 8, 20), 4, 0.0, 0.0, 0.0, "No"),

                  (None, date(2019, 8, 13), "GTE_UK_BPGM", "IR_LIBOR.GBP", date(2019, 8, 21),
                   date(2019, 9, 13), 341301.68002532, 0.0071275,
                   0.0071225, -1.51543062, -4.0E-7, -1.51543062, -4.0E-7, -1.82882167, -4.8E-7, 3.4130168, -3.84E-6,
                   "'1m", date(2019, 9, 13), 7, 0.0, 0.0, 0.0, "No"),

                  (None, date(2019, 8, 13), "GTE_UK_BPGM", "IR_LIBOR.GBP", date(2019, 9, 14),
                   date(2019, 10, 14), 124313.50303927, 0.0074388,
                   0.0074313, -0.94614137, -1.07E-6, -0.94614137, -1.07E-6, -1.1418034, -1.29E-6, 1.24313503, -3.87E-6,
                   "'2m", date(2019, 10, 14), 8, 0.0, 0.0, 0.0, "No"),
                  ]

    expected_data = [
        (date(2019, 8, 13), "GTE_UK_BPGM", "IR_LIBOR.GBP", date(2019, 8, 17), date(2019, 8, 20), -64217.1037641,
         0.00693, 0.0069325, -0.1586031, 1e-08, -0.1586031, 1e-08, -0.19140222, 1e-08, -0.64217104,
         3.3e-07, "'1w", date(2019, 8, 20), 4, 0.0, 0.0, 0.0, "No", None),

        (date(2019, 8, 13), "GTE_UK_BPGM", "IR_LIBOR.GBP", date(2019, 8, 21), date(2019, 9, 13), 341301.68002532,
         0.0071275, 0.0071225, -1.51543062, -4e-07, -1.51543062, -4e-07, -1.82882167, -4.8e-07, 3.4130168,
         -3.84e-06, "'1m", date(2019, 9, 13), 7, 0.0, 0.0, 0.0, "No", None),

        (date(2019, 8, 13), "GTE_UK_BPGM", "IR_LIBOR.GBP", date(2019, 9, 14), date(2019, 10, 14), 124313.50303927,
         0.0074388, 0.0074313, -0.94614137, -1.07e-06, -0.94614137, -1.07e-06, -1.1418034, -1.29e-06, 1.24313503,
         -3.87e-06, "'2m", date(2019, 10, 14), 8, 0.0, 0.0, 0.0, "No", None),
    ]

    df = spark_session.createDataFrame(data=input_data, schema=typed_output_schema)
    df_actual = dtd_calc(df)

    df_expected = spark_session.createDataFrame(data=expected_data, schema=transformed_output_schema)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())
