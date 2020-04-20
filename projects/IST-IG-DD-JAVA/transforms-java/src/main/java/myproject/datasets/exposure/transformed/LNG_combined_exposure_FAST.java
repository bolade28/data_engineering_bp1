package myproject.datasets.exposure.transformed;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import com.palantir.transforms.lang.java.api.TransformContext;

import myproject.datasets.exposure.current.dl130_market_risk_stage_result_summary;
import myproject.datasets.exposure.current.tr200_exposure;
import myproject.datasets.exposure.current.fp_gas_m0145;
import myproject.datasets.exposure.current.fp_power_m01015;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import java.util.Arrays;
import java.util.Date;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import myproject.datasets.util.BP_DatasetUtils;
import scala.collection.Seq;
import scala.collection.JavaConverters;


/**
 * This is an example high-level Transform intended for automatic registration.
 */
public final class LNG_combined_exposure_FAST
{
    private static final Logger LOG = LoggerFactory.getLogger(LNG_combined_exposure_FAST.class);

    Dataset<Row> convert_titan(Dataset<Row> df_typed,
                               Dataset<Row> df_ref_titan_curve,
                               Dataset<Row> df_ref_titan_conv,
                               Dataset<Row> df_ref_uom_conversions,
                               Dataset<Row> df_ref_conformed_units )
    {

        // current
        Dataset<Row> df_current = new tr200_exposure().convert(df_typed);

        // conformed
        Dataset<Row> df_conformed = new tr200_exposure_conformed().convert(df_current,
                df_ref_titan_curve,
                df_ref_titan_conv,
                df_ref_uom_conversions,
                df_ref_conformed_units
        );

        // combined
        Dataset<Row> df = new tr200_combined_format().convert(df_conformed);
        return df;
    }

    Dataset<Row> convert_endur(TransformContext context,
                               Dataset<Row> df_typed,
                               Dataset<Row> df_endur_option_curves,
                               Dataset<Row> df_endur_curves,
                               Dataset<Row> df_ref_uom_conversions,
                               Dataset<Row> df_ref_conformed_units,
                               Dataset<Row> df_endur_counterparties
                               )
    {
        // current
        Dataset<Row> df_current = new dl130_market_risk_stage_result_summary().convert(df_typed);

        // monnthly
        Dataset<Row> df_monthly = new dl130_market_risk_stage_result_summary_monthly().convert(context, df_current);

        // conformed
        Dataset<Row> df_conformed = new dl130_market_risk_stage_result_summary_conformed().convert(df_monthly,df_ref_uom_conversions, df_endur_counterparties);

        // combined
        Dataset<Row> df = new dl130_Endur_Monthly_Exposure_combined_format().convert(df_conformed,
                df_endur_option_curves,
                df_endur_curves,
                df_ref_uom_conversions,
                df_ref_conformed_units
                );

        // combined
        return df;
    }


    Dataset<Row> convert_fp_gas(Dataset<Row> df_typed,
                                Dataset<Row> df_ref_nagp_curves,
                                Dataset<Row> df_ref_nagp_counterparties,
                                Dataset<Row> df_ref_uom_conversions,
                                Dataset<Row> df_ref_conformed_units,
                                Dataset<Row> df_ref_nagp_uoms
    )
    {
        // current
        Dataset<Row> df_current = new fp_gas_m0145().convert(df_typed);

        // cleansed
        Dataset<Row> df_cleansed = new fp_gas_m0145_cleansed().convert(
                df_current
                , df_ref_nagp_counterparties
                , df_ref_nagp_uoms
                );

        // combined
        Dataset<Row> df = new fp_gas_m0145_combined_format().convert( df_cleansed
                , df_ref_nagp_curves
                , df_ref_uom_conversions
                , df_ref_conformed_units
        );
        return df;
    }

    Dataset<Row> convert_fp_power(Dataset<Row> df_typed, Dataset<Row> df_ref_nagp_curves)
    {
        // current
        Dataset<Row> df_current = new fp_power_m01015().convert(df_typed);

        // cleansed
        Dataset<Row> df_cleansed = new fp_power_m01015_cleansed().convert(df_current);

        // combined
        Dataset<Row> df = new fp_power_m01015_combined_format().convert(df_cleansed, df_ref_nagp_curves);

        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/transformed/LNG_combined_exposure_FAST")
    public Dataset<Row> myComputeFunction(TransformContext context,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/titan/tr200_exposure") Dataset<Row> df_titan_input,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/endur/dl130_market_risk_stage_result_summary") Dataset<Row> df_endur_input,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/freeport/fp_power_m01015") Dataset<Row> df_fp_power_input,
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/freeport/fp_gas_m0145") Dataset<Row> df_fp_gas_input,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Titan_Curves") Dataset<Row> df_ref_titan_curve,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Titan_Oil_UOM_Conversions") Dataset<Row> df_ref_titan_conv,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/UOM_Conversions") Dataset<Row> df_ref_uom_conversions,
            @Input("/BP/IST-IG-SS-Systems/data/refdata/conversion_uom_pivotted") Dataset<Row> df_pivoted_conversions,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Conformed_Units") Dataset<Row> df_ref_conformed_units,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Curves") Dataset<Row> df_ref_nagp_curves,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_Counterparties") Dataset<Row> df_ref_nagp_counterparties,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/NAGP_UOMs") Dataset<Row> df_ref_nagp_uoms,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Curves") Dataset<Row> df_endur_curves,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Options_Curves") Dataset<Row> df_endur_option_curves,
            @Input("/BP/IST-IG-UC-User-Data/data/referencedata/Endur_Counterparties") Dataset<Row> df_endur_counterparties

    )
    {
        LOG.info("--- start ----");
        Dataset<Row> df_endur_output = convert_endur(
                context,
                df_endur_input,
                df_endur_option_curves,
                df_endur_curves,
                df_ref_uom_conversions,
                df_ref_conformed_units,
                df_endur_counterparties
        );
        LOG.info("convert_endur complete");
        
        Dataset<Row> df_titan_output = convert_titan(
                df_titan_input,
                df_ref_titan_curve,
                df_ref_titan_conv,
                df_ref_uom_conversions,
                df_ref_conformed_units
        );
        LOG.info("convert_titan complete");

        Dataset<Row> df_fp_power_output = convert_fp_power(df_fp_power_input, df_ref_nagp_curves);
        LOG.info("convert_fp_power complete");

        Dataset<Row> df_fp_gas_output = convert_fp_gas(df_fp_gas_input,
                df_ref_nagp_curves,
                df_ref_nagp_counterparties,
                df_ref_uom_conversions,
                df_ref_conformed_units,
                df_ref_nagp_uoms);
        LOG.info("convert_fp_gas complete");
        
        Dataset<Row> df = new LNG_combined_exposure().convert(df_titan_output, df_endur_output, df_fp_power_output, df_fp_gas_output);
        df = df.filter(df.col("Fobus_Curve_Name").notEqual("n/a"));
        LOG.info("LNG_combined_exposure().convert complete");
        LOG.info("--- end ----");
        return df;
    }
}
