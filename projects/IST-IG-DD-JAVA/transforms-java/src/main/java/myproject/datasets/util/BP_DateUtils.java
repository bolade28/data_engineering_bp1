package myproject.datasets.util;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.BufferLike;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.aggregate.Max;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This is an example high-level Transform intended for automatic registration.
 */
public final class BP_DateUtils {

    public static Date[] getStartEndDate() {
        Calendar cal = Calendar.getInstance();
        cal.set(2019, 5, 25); // month in 0 index, Jan = 0
        Date startDate = new Date(cal.getTimeInMillis());
        cal.set(2020, 11, 18);
        Date endDate = new Date(cal.getTimeInMillis());
        return new Date[] { startDate, endDate };
    }

    public static Dataset<Row> filterLatest(Dataset<Row> df, Date startDate, Date endDate, String valuationDateCol,
            String rundatetimeCol) {
        // The compute function for a high-level Transform returns an output of type
        // "Dataset<Row>".
        df = df.filter(row -> row.<Date>getAs(valuationDateCol).after(startDate)
                && row.<Date>getAs(valuationDateCol).before(endDate));
        Dataset<Row> df_latest_runtimes = df.select(valuationDateCol, rundatetimeCol).groupBy(valuationDateCol)
                .agg(Collections.singletonMap(rundatetimeCol, "max"));
        Calendar cal = Calendar.getInstance();
        df_latest_runtimes = df_latest_runtimes.withColumnRenamed(df_latest_runtimes.columns()[1], rundatetimeCol);
        Seq colSeq = JavaConverters.asScalaBufferConverter(Arrays.asList(rundatetimeCol, valuationDateCol)).asScala()
                .seq();
        df = df.join(df_latest_runtimes, colSeq);
        return df;
    }

    public static class MonthlyBucket {
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        String date = simpleDateFormat.format(new Date());

        public MonthlyBucket(Date start_date, Date end_date, int num_days) {
            super();
            this.start_date = start_date;
            this.end_date = end_date;
            this.num_days = num_days;
        }

        public Date start_date;
        public Date end_date;
        public int num_days;

        // Any slight changes to the following method will have a major impact on
        // the result of the monthly_bucket computation. So change with care.
        @Override
        public String toString() {
            return new StringBuilder().append(simpleDateFormat.format(start_date)).append("#")
                    .append(simpleDateFormat.format(end_date)).append("#").append(num_days).toString();
        }

    }

    public static Date last_day_of_month(Date d) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        int day = cal.getActualMaximum(Calendar.DATE);
        cal.set(Calendar.DATE, day);
        return cal.getTime();
    }

    public static Date first_day_of_month(Date d) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        int day = cal.getActualMinimum(Calendar.DATE);
        cal.set(Calendar.DATE, day);
        return cal.getTime();
    }

    public static Date add_days(Date d, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    public static int days_between(Date d1, Date d2) {
        long diff_in_millis = d2.getTime() - d1.getTime();
        int retval = (int) (diff_in_millis / (1000 * 60 * 60 * 24));
        return retval;
    }

    public static Seq<String> calc_monthly_buckets(Date start_date, Date end_date) {
        List<String> retval = new ArrayList<>();
        Date curr_date = start_date;
        while (!curr_date.after(end_date)) {
            Date eom = last_day_of_month(curr_date);
            if (eom.after(end_date)) {
                eom = end_date;
            }
            int num_days = days_between(curr_date, eom) + 1;

            retval.add((new MonthlyBucket(curr_date, eom, num_days).toString()));
            curr_date = add_days(eom, 1);

        }
        return JavaConverters.asScalaBufferConverter(retval).asScala().toSeq();

    }

}
