package myproject.datasets.exposure.current;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Date;

public class fp_power_m01015 {
    public Dataset<Row> convert(Dataset<Row> df) {
        Date[] dates = BP_DateUtils.getStartEndDate();
        df = BP_DateUtils.filterLatest(df, dates[0], dates[1], "PLEX_Calculation_Date", "Run_Datetime");
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/current/freeport/fp_power_m01015")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/freeport/fp_power_m01015") Dataset<Row> df
    ){
        return convert(df);
    }

}
