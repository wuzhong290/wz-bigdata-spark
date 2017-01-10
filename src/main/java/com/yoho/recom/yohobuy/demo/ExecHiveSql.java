package com.yoho.recom.yohobuy.demo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * Created by dell on 2016/12/21.
 */
public class ExecHiveSql {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage ExecHiveSql sparkMaster sql");
        }
        String master = args[0];
        String sql = args[1];
        System.out.println(System.getenv("SPARK_HOME")+":"+sql);
        JavaSparkContext sc = new JavaSparkContext(
                master, "ExecHiveSql", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        HiveContext sqlContext = new HiveContext(sc.sc());
        DataFrame rdd = sqlContext.sql(sql);
        List<Row> rdds = rdd.collectAsList();
        for (Row row: rdds) {
            System.out.println(row.toString());
        }
    }
}
