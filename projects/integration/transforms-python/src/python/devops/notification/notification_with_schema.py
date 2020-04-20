# ===== import: python function

# ===== import: palantir functions
from transforms.api import transform_df, Input, Output, incremental, configure
# ===== import: our functions
from python.util.bp_devops import identify_processed_dataset


@configure(profile=['NUM_EXECUTORS_16'])
@incremental(snapshot_inputs=['df_300', 'df_310', 'df_055', 'df_030', 'df_035', 'df_070', 'df_120', 'df_125', 'df_130',
             'fp_gas_m145', 'fp_power_m145', 'fp_power_m01015', 'tr_200'])
@transform_df(
    Output("/BP/IST-IG-DD/data/technical/devops/notification/notifications_with_schema"),
    df_300=Input("/BP/IST-IG-DD/data/technical/prices/typed/endur/dl300_endur_forward_curves"),
    raw_df_300=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl300-endur_forward_curves/dl300-endur_forward_curves"),
    df_310=Input("/BP/IST-IG-DD/data/technical/prices/typed/endur/dl310_endur_volatilities"),
    raw_df_310=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl310-endur_volatilities/dl310-endur_volatilities"),
    df_055=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl055-pnl_adjustments"),
    raw_df_055=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl055-pnl_adjustments/dl055-pnl_adjustments"),
    df_030=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl030-pnl_detail_ytd"),
    raw_df_030=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl030-pnl_detail_ytd/dl030-pnl_detail_ytd"),
    df_035=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl035-pnl_by_portfolio_and_instrument"),
    raw_df_035=Input(
     "/BP/IST-IG-SS-Systems/data/raw/endur/dl035-pnl_by_portfolio_and_instrument/dl035-pnl_by_portfolio_and_instrument"
        ),
    df_070=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl070-pnl_explained_by_gridpoint"),
    raw_df_070=Input(
        "/BP/IST-IG-SS-Systems/data/raw/endur/dl070-pnl_explained_by_gridpoint/dl070-pnl_explained_by_gridpoint"),
    df_075=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl075-pnl_explained_by_portfolio_and_instrument"),
    raw_df_075=Input(
     "/BP/IST-IG-SS-Systems/data/raw/endur/dl075-pnl_explained_by_portfolio_and_instrument/dl075-pnl_explained_by_portfolio_and_instrument"),
    df_120=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl120-market_risk_stage_result"),
    raw_df_120=Input("/BP/IST-IG-SS-Systems/data/raw/endur/dl120-market_risk_stage_result/dl120-market_risk_stage_result"),
    df_125=Input("/BP/IST-IG-DD/data/technical/pnl/typed/endur/dl125-market_risk_stage_result_emission"),
    raw_df_125=Input(
     "/BP/IST-IG-SS-Systems/data/raw/endur/dl125-market_risk_stage_result_emission/dl125-market_risk_stage_result_emission"),
    df_130=Input("/BP/IST-IG-DD/data/technical/exposure/typed/endur/dl130_market_risk_stage_result_summary"),
    raw_df_130=Input(
        "/BP/IST-IG-SS-Systems/data/raw/endur/dl130-market_risk_stage_result_summary/dl130_market_risk_stage_result_summary"),
    fp_gas_m145=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_gas_m0145"),
    raw_fp_gas_m145=Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_gas_m0145/fp_gas_m0145"),
    fp_power_m145=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m0145"),
    raw_fp_power_m145=Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_power_m0145/fp_power_m0145"),
    fp_power_m01015=Input("/BP/IST-IG-DD/data/technical/exposure/typed/freeport/fp_power_m01015"),
    raw_fp_power_m01015=Input("/BP/IST-IG-SS-Systems/data/raw/freeport/fp_power_m01015/fp_power_m01015"),
    tr_200=Input("/BP/IST-IG-DD/data/technical/exposure/typed/titan/tr200_exposure"),
    raw_tr_200=Input("/BP/IST-IG-SS-Systems/data/raw/titan/tr200_exposure/tr200_exposure"),

)
def dtd_calc(df_300, raw_df_300, df_310, raw_df_310, df_055, raw_df_055, df_030, raw_df_030, df_035, raw_df_035, df_070,
             raw_df_070, df_075, raw_df_075, df_120, raw_df_120, df_125, raw_df_125, df_130, raw_df_130, fp_gas_m145,
             raw_fp_gas_m145, fp_power_m145, raw_fp_power_m145, fp_power_m01015, raw_fp_power_m01015, tr_200,
             raw_tr_200):
    """ Transformation stage function
    This function:
        Version: T1
        Reads the distinct value of reval_date from input raw and transformed
        then write into a new data set with differences

    Args:
        params1 (dataframe): typed dataset
        params2 (dataframe): Raw new file dataset
        params3 (String)   : Name of the dataset to go in the notification DS for identification

    Returns:
        dataframe: old dataset
    """
    # calling function to identify the matches and write to a new output
    # dl300
    df = identify_processed_dataset(raw_df_300, df_300, 'DL300', "Valuation_Date")
    # dl310
    df_310 = identify_processed_dataset(raw_df_310, df_310, 'DL310', "Valuation_Date")
    df = df.union(df_310)
    # dl055
    df_055 = identify_processed_dataset(raw_df_055, df_055, 'DL055', "Reval_Date")
    df = df.union(df_055)
    # dl030
    df_030 = identify_processed_dataset(raw_df_030, df_030, 'DL030', "Reval_Date")
    df = df.union(df_030)
    # dl035
    df_035 = identify_processed_dataset(raw_df_035, df_035, 'DL035', "Reval_Date")
    df = df.union(df_035)
    # dl070
    df_070 = identify_processed_dataset(raw_df_070, df_070, 'DL070', "Reval_Date")
    df = df.union(df_070)
    # dl075
    df_075 = identify_processed_dataset(raw_df_075, df_075, 'DL075', "Reval_Date")
    df = df.union(df_075)
    # dl120
    df_120 = identify_processed_dataset(raw_df_120, df_120, 'DL120', "Analysis_Date")
    df = df.union(df_120)
    # dl125
    df_125 = identify_processed_dataset(raw_df_125, df_125, 'DL120', "Analysis_Date")
    df = df.union(df_125)
    # expo dl_130
    df_130 = identify_processed_dataset(raw_df_130, df_130, 'DL130', "Analysis_Date")
    df = df.union(df_130)
    # expo gas m145
    fp_gas_m145 = identify_processed_dataset(raw_fp_gas_m145, fp_gas_m145, 'fp_gas_m145', "Analysis_Date")
    df = df.union(fp_gas_m145)
    # expo power m145
    fp_power_m145 = identify_processed_dataset(raw_fp_power_m145, fp_power_m145, 'fp_power_m145', "Analysis_Date")
    df = df.union(fp_power_m145)
    # expo power m01015
    fp_power_m01015 = identify_processed_dataset(raw_fp_power_m01015, fp_power_m01015,
                                                 'fp_power_m145', "PLEX_Calculation_Date")
    df = df.union(fp_power_m01015)
    # expo TR 200
    tr_200 = tr_200.withColumnRenamed("RunDateTime", "Run_Datetime")
    raw_tr_200 = raw_tr_200.withColumnRenamed("RunDateTime", "Run_Datetime")
    tr_200 = identify_processed_dataset(raw_tr_200, tr_200, 'tr_200', "Current_COB_Date")
    df = df.union(tr_200)
    return df
