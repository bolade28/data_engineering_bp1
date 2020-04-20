from pandas.testing import assert_frame_equal
from python.exposure.transformed.expo040_lng_combined_for_limits import process_nymex, processor
from python.exposure.schema_exposure.schema_expo040_combined_for_limits import combined_output_schema
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, DateType, TimestampType
from datetime import datetime, date


ref_schema = StructType([
    StructField('Parameter_Type', StringType(), True),
    StructField('Name', StringType(), True),
    StructField('Value', DoubleType(), True),
    StructField('Start_Date', DateType(), True),
    StructField('End_Date', DateType(), True),
    StructField('Last_Updated_Timestamp', TimestampType(), True),
    StructField('Last_Updated_User', StringType(), True)
])


ref_data = [('JCC Multipliers', 'M1', 0.78, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0), 'system'),
            ('JCC Multipliers', 'M2', 0.268, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0), 'system'),
            ('JLC Multipliers', 'M4', 0.057891, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0), 'system'),
            ('JLC Multipliers', 'M5', 0.053779, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0), 'system'),
            ('Nymex Hub Volumetric', 'NG_NYMEX', 1.0, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0), 'system'),
            ('Nymex Hub Volumetric', 'NG_ML_Monthly', -1.0, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0),
             'system'),
            ('Nymex Hub Volumetric', 'NG_Katy_Monthly', -1.0, date(2019, 1, 1), None, datetime(2019, 1, 1, 15, 0, 0),
             'system')]

data = [('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_NYMEX', 'JCC', 'JCC', 536000.0, 'BBL', None,
            None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0, -3792370.0, None,
            date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020, 'M+3', 1847, 4, 7,
            'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None, 'BP LNG Physical Trading London',
            'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active', None, None, 'ENGY-SWAP', 'BASE', None, None,
            None, None, None, None, None, 'Yes', datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0)),
        ('Titan', date(2019, 8, 29), 'PORTFOLIO', 'JLC FORWARD', 'JLC', 'JLC', -1200000.0, 'MMBtu', -1200000.0,
            'MMBtu', 0.0, -351685.0, -1200000.0, 0.0, 0.0, 0.0, 0., 0., 0., 0., 1.0, date(2022, 4, 1),
            date(2022, 4, 30), 'Apr 2022', '2022 Q2', '2022 S', 2022, None, None, None, None, None, None, None,
            'Osaka Gas Co., Ltd.', 'LNG - BP Singapore Pte. Ltd.', None, None, 8.798128, 'USD/MMBtu', 'Active',
            date(2019, 7, 23), 129444, None, None, 'Cargo', 'Curve Price Commodity UOM', 7, 2022, 'Sakai LNG',
            'OG T2 2016-37', 'Strategic', 'Yes', datetime(2019, 8, 30, 7, 14, 21), datetime(2019, 1, 1, 15, 0, 0)),
        ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_ML_Monthly', 'JCC', 'JCC', 536000.0, 'BBL', None,
            None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0, -3792370.0, None,
            date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020, 'M+3', 1847, 4, 7,
            'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None, 'BP LNG Physical Trading London',
            'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active', None, None, 'ENGY-SWAP', 'BASE', None, None,
            None, None, None, None, None, 'Yes', datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0)),
        ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_Katy_Monthly', 'JCC', 'JCC', 536000.0, 'BBL',
            None, None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0, -3792370.0, None,
            date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020, 'M+3', 1847, 4, 7,
            'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None, 'BP LNG Physical Trading London',
            'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active', None, None, 'ENGY-SWAP', 'BASE', None, None,
            None, None, None, None, None, 'Yes', datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0))]

expected_data = [('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_NYMEX', 'JCC', 'JCC', 536000.0, 'BBL',
                    None, None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0, -3792370.0,
                    None, date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020, 'M+3', 1847, 4,
                    7, 'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None, 'BP LNG Physical Trading London',
                    'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active', None, None, 'ENGY-SWAP', 'BASE',
                    None, None, None, None, None, None, None, 'Yes', datetime(2020, 3, 6, 5, 32, 39),
                    datetime(2019, 1, 1, 15, 0, 0)),
                    ('Titan', date(2019, 8, 29), 'PORTFOLIO', 'JLC FORWARD', 'JLC', 'JLC', -1200000.0, 'MMBtu',
                    -1200000.0, 'MMBtu', 0.0, -351685.0, -1200000.0, 0.0, 0.0, 0.0, 0., 0., 0., 0., 1.0,
                    date(2022, 4, 1), date(2022, 4, 30), 'Apr 2022', '2022 Q2', '2022 S', 2022, None, None, None,
                    None, None, None, None, 'Osaka Gas Co., Ltd.', 'LNG - BP Singapore Pte. Ltd.', None, None,
                    8.798128, 'USD/MMBtu', 'Active', date(2019, 7, 23), 129444, None, None, 'Cargo',
                    'Curve Price Commodity UOM', 7, 2022, 'Sakai LNG', 'OG T2 2016-37', 'Strategic', 'Yes',
                    datetime(2019, 8, 30, 7, 14, 21), datetime(2019, 1, 1, 15, 0, 0)),
                    ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_ML_Monthly', 'JCC', 'JCC', 536000.0,
                    'BBL', None, None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0,
                    -3792370.0, None, date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020,
                    'M+3', 1847, 4, 7, 'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None,
                    'BP LNG Physical Trading London', 'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active',
                    None, None, 'ENGY-SWAP', 'BASE', None, None, None, None, None, None, None, 'Yes',
                    datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0)),
                    ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_Katy_Monthly', 'JCC', 'JCC', 536000.0,
                    'BBL', None, None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0,
                    -3792370.0, None, date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020,
                    'M+3', 1847, 4, 7, 'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None,
                    'BP LNG Physical Trading London', 'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active',
                    None, None, 'ENGY-SWAP', 'BASE', None, None, None, None, None, None, None, 'Yes',
                    datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0)),
                    # Added Rows
                    ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_Katy_Monthly', 'Nymex Hub Volumetric',
                    'JCC', -536000.0, 'BBL', None, None, -3.216E7, -911200.0, -3216000.0, -536000.0, None, None, 0.0,
                    0.0, -113.0, -3792370.0, None, date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2',
                    '2020 S', 2020, 'M+3', 1847, 4, 7, 'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None,
                    'BP LNG Physical Trading London', 'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active',
                    None, None, 'ENGY-SWAP', 'BASE', None, None, None, None, None, None, None, 'Yes',
                    datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0)),
                    ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_ML_Monthly', 'Nymex Hub Volumetric',
                    'JCC', -536000.0, 'BBL', None, None, -3.216E7, -911200.0, -3216000.0, -536000.0, None, None, 0.0,
                    0.0, -113.0, -3792370.0, None, date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2',
                    '2020 S', 2020, 'M+3', 1847, 4, 7, 'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None,
                    'BP LNG Physical Trading London', 'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active',
                    None, None, 'ENGY-SWAP', 'BASE', None, None, None, None, None, None, None, 'Yes',
                    datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0)),
                    ('Endur', date(2020, 3, 5), 'LNG_LON_PHYS_OIL_CRUDE', 'NG_NYMEX', 'Nymex Hub Volumetric', 'JCC',
                    536000.0, 'BBL', None, None, 3.216E7, 911200.0, 3216000.0, 536000.0, None, None, 0.0, 0.0, -113.0,
                    -3792370.0, None, date(2020, 6, 1), date(2020, 6, 30), 'Jun 2020', '2020 Q2', '2020 S', 2020,
                    'M+3', 1847, 4, 7, 'Jun-20/fwd-m', date(2020, 6, 1), date(2020, 6, 30), None,
                    'BP LNG Physical Trading London', 'LNG_LON_PHYS-BU', 'LNG_LON_PHYS_OIL_PH', 0.0, None, 'Active',
                    None, None, 'ENGY-SWAP', 'BASE', None, None, None, None, None, None, None, 'Yes',
                    datetime(2020, 3, 6, 5, 32, 39), datetime(2019, 1, 1, 15, 0, 0))]


def test_process_nymex(spark_session):

    delta_cols = ["MR_Delta", "MR_Delta_Conformed", "MR_Delta_Therms", "MR_Delta_MWh", "MR_Delta_MMBtu", "MR_Delta_BBL",
                  "MR_Delta_EUR", "MR_Delta_GBP"]

    df = spark_session.createDataFrame(data=data, schema=combined_output_schema)
    ref_data_df = spark_session.createDataFrame(data=ref_data, schema=ref_schema)
    df_expected = spark_session.createDataFrame(data=expected_data, schema=combined_output_schema)
    df_actual = process_nymex(df, ref_data_df, delta_cols)

    # Test that the two schemas are the same
    assert(df_actual.schema == df_expected.schema)

    # Test that the content of the two dataframes are equal
    assert_frame_equal(df_actual.toPandas(), df_expected.toPandas())


def test_process_jcc_jlc(spark_session):

    df = spark_session.createDataFrame(data=data, schema=combined_output_schema)
    ref_data_df = spark_session.createDataFrame(data=ref_data, schema=ref_schema)

    df_out = processor(df, ref_data_df)
    rows = df_out.collect()
    # Test that all relevant deltas complies to double fields but appears as integer.
    for row in rows:
        assert len(str(row['MR_Delta']).split('.')) == 1
        assert len(str(row['MR_Delta_Conformed']).split('.')) == 1
        assert len(str(row['MR_Delta_Therms']).split('.')) == 1
        assert len(str(row['MR_Delta_MWh']).split('.')) == 1
        assert len(str(row['MR_Delta_MMBtu']).split('.')) == 1
        assert len(str(row['MR_Delta_BBL']).split('.')) == 1
        assert len(str(row['MR_Delta_EUR']).split('.')) == 1
        assert len(str(row['MR_Delta_GBP']).split('.')) == 1

