package myproject.datasets.exposure.current;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;

import java.util.Date;

public class dl130_market_risk_stage_result_summary {

    public Dataset<Row> convert(Dataset<Row> df) {
        Date[] dates = BP_DateUtils.getStartEndDate();
        df = BP_DateUtils.filterLatest(df, dates[0], dates[1], "Analysis_Date", "Run_Datetime");
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/current/endur/dl130_market_risk_stage_result_summary")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/endur/dl130_market_risk_stage_result_summary") Dataset<Row> df
    ){
        return convert(df);
    }

}

