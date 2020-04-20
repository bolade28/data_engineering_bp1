from transforms.api import transform, Input, Output, incremental
import pyspark.sql.functions as f
from python.pnl.typed import utils
from python.pnl.schema.schema_tr160_pnl_output import typed_output_schema

import logging
logger = logging.getLogger(__name__)


# @incremental()
@transform(
    output_data=Output("/BP/IST-IG-DD/data/technical/pnl/typed/titan/tr160-pnl_output"),
    input_data=Input("/BP/IST-IG-SS-Systems/data/raw/titan/tr160-pnl_output/tr160-pnl_output"),
)
def my_compute_function(ctx, input_data, output_data):
    '''
    Version: V1 (Python reader)
    '''
    expected_fields = list(map(lambda x: x.name, typed_output_schema))
    try:
        prev_col_names = output_data.dataframe().columns
    except Exception:
        prev_col_names = []

    df = utils.build_input_data(ctx, input_data, prev_col_names, expected_fields)
    if df:
        df = df.select(
            df['Cumulative_Run_ID'].cast('integer'),
            df['Version'].cast('integer'),
            f.to_date(df['COB_Date'], 'yyy-MM-dd').alias('COB_Date'),
            df['Valuation_Definition'],
            df['Report_Type'],
            df['Valuation_Key'],
            df['Trade_Group_ID'].cast('integer'),
            df['Trade_ID'].cast('integer'),
            df['Trade_Settlement_Period_ID'].cast('integer'),
            df['Non_Trade_Valuation_Key'],
            df['Book'],
            df['Legal_Entity'],
            df['Counterparty'],
            df['Product_Type'],
            df['Charge_Type'],
            df['Cargo_Product'],
            df['Leg_Type'],
            df['Buy_Sell'],
            df['Trade_Currency'],
            df['Reporting_Currency'],
            df['DTD_Total_Disc_Deal'].cast('double'),
            df['DTD_Real_Disc_Deal'].cast('double'),
            df['DTD_Unreal_Disc_Deal'].cast('double'),
            df['DTD_Total_Undisc_Deal'].cast('double'),
            df['DTD_Real_Undisc_Deal'].cast('double'),
            df['DTD_Unreal_Undisc_Deal'].cast('double'),
            df['DTD_Total_Disc_Pf'].cast('double'),
            df['DTD_Real_Disc_Pf'].cast('double'),
            df['DTD_Unreal_Disc_Pf'].cast('double'),
            df['DTD_Total_Undisc_Pf'].cast('double'),
            df['DTD_Real_Undisc_Pf'].cast('double'),
            df['DTD_Unreal_Undisc_Pf'].cast('double'),
            df['MTD_Total_Disc_Deal'].cast('double'),
            df['MTD_Real_Disc_Deal'].cast('double'),
            df['MTD_Unreal_Disc_Deal'].cast('double'),
            df['MTD_Total_Undisc_Deal'].cast('double'),
            df['MTD_Real_Undisc_Deal'].cast('double'),
            df['MTD_Unreal_Undisc_Deal'].cast('double'),
            df['MTD_Total_Disc_Pf'].cast('double'),
            df['MTD_Real_Disc_Pf'].cast('double'),
            df['MTD_Unreal_Disc_Pf'].cast('double'),
            df['MTD_Total_Undisc_Pf'].cast('double'),
            df['MTD_Real_Undisc_Pf'].cast('double'),
            df['MTD_Unreal_Undisc_Pf'].cast('double'),
            df['QTD_Total_Disc_Deal'].cast('double'),
            df['QTD_Real_Disc_Deal'].cast('double'),
            df['QTD_Unreal_Disc_Deal'].cast('double'),
            df['QTD_Total_Undisc_Deal'].cast('double'),
            df['QTD_Real_Undisc_Deal'].cast('double'),
            df['QTD_Unreal_Undisc_Deal'].cast('double'),
            df['QTD_Total_Disc_Pf'].cast('double'),
            df['QTD_Real_Disc_Pf'].cast('double'),
            df['QTD_Unreal_Disc_Pf'].cast('double'),
            df['QTD_Total_Undisc_Pf'].cast('double'),
            df['QTD_Real_Undisc_Pf'].cast('double'),
            df['QTD_Unreal_Undisc_Pf'].cast('double'),
            df['YTD_Total_Disc_Deal'].cast('double'),
            df['YTD_Real_Disc_Deal'].cast('double'),
            df['YTD_Unreal_Disc_Deal'].cast('double'),
            df['YTD_Total_Undisc_Deal'].cast('double'),
            df['YTD_Real_Undisc_Deal'].cast('double'),
            df['YTD_Unreal_Undisc_Deal'].cast('double'),
            df['YTD_Total_Disc_Pf'].cast('double'),
            df['YTD_Real_Disc_Pf'].cast('double'),
            df['YTD_Unreal_Disc_Pf'].cast('double'),
            df['YTD_Total_Undisc_Pf'].cast('double'),
            df['YTD_Real_Undisc_Pf'].cast('double'),
            df['YTD_Unreal_Undisc_Pf'].cast('double'),
            df['LTD_Total_Disc_Deal'].cast('double'),
            df['LTD_Real_Disc_Deal'].cast('double'),
            df['LTD_Unreal_Disc_Deal'].cast('double'),
            df['LTD_Total_Undisc_Deal'].cast('double'),
            df['LTD_Real_Undisc_Deal'].cast('double'),
            df['LTD_Unreal_Undisc_Deal'].cast('double'),
            df['LTD_Total_Disc_Pf'].cast('double'),
            df['LTD_Real_Disc_Pf'].cast('double'),
            df['LTD_Unreal_Disc_Pf'].cast('double'),
            df['LTD_Total_Undisc_Pf'].cast('double'),
            df['LTD_Real_Undisc_Pf'].cast('double'),
            df['LTD_Unreal_Undisc_Pf'].cast('double'),
            f.to_timestamp(df['Run_Datetime'], 'yyyy-MM-dd HH:mm:ss').alias('Run_Datetime'),
        )
        output_data.write_dataframe(df)
    else:
        logger.info("=========== EMPTY DF")
