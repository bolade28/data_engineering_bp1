package myproject.datasets.exposure.current;

import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import myproject.datasets.util.BP_DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Date;

public class tr200_exposure {
    public Dataset<Row> convert(Dataset<Row> df) {
        Date[] dates = BP_DateUtils.getStartEndDate();
        df = BP_DateUtils.filterLatest(df, dates[0], dates[1], "Current_COB_Date", "RunDateTime");
        return df;
    }

    @Compute
    @Output("/BP/IST-IG-DD/data/technical/exposure_java/current/titan/tr200_exposure")
    public Dataset<Row> myComputeFunction(
            @Input("/BP/IST-IG-DD/data/technical/exposure_java/typed/titan/tr200_exposure") Dataset<Row> df
    ){
        return convert(df);
    }
}
