package com.yoho.recom.yohobuy.userHistory;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Date;

/**
 * Created by dell on 2016/12/21.
 */
public class GlbUserHistoryCart {

    public static class ParseLine implements Function<Row, String[]> {
        public String[] call(Row row) throws Exception {
            return row.toString().split(",");
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage GlbUserHistoryCart sparkMaster csvOutputFile");
        }
        String master = args[0];
        String outputFile = args[1];
        String endDateDiff = args[2];
        String beginDateDiff = args[3];
        Date now = new Date();
        Date yesterday = DateUtils.addDays(now,-1);
        String endDate = DateFormatUtils.format(DateUtils.addDays(yesterday,Integer.parseInt(endDateDiff)),"yyyyMMdd");
        String beginDate = DateFormatUtils.format(DateUtils.addDays(yesterday,Integer.parseInt(endDateDiff+beginDateDiff)),"yyyyMMdd");

        JavaSparkContext sc = new JavaSparkContext(
                master, "GlbUserHistoryCart", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        HiveContext sqlContext = new HiveContext(sc.sc());
        String sql1 = "select user_log_acct as uid," +
                "date_id as view_date" +
                " from recom_yohobuy.add_cart_info_global" +
                " where date_id <=" + endDate +
                " and date_id > " + beginDate +
                " and user_log_acct is not null" +
                " group by user_log_acct, date_id";
        DataFrame rdd = sqlContext.sql(sql1);
        JavaRDD<String[]> rdds = rdd.toJavaRDD().map(new ParseLine());

        rdds.saveAsTextFile(outputFile);
    }
}
