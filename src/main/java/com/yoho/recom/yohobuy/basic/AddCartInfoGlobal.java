package com.yoho.recom.yohobuy.basic;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * Created by wuzhong on 2016/12/21.
 * 执行命令：spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master local --executor-memory 1G --total-executor-cores 1 /var/lib/hadoop-hdfs/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar local
 *scp yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar hadoop@spark01:/home/hadoop
 *spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master spark://172.31.54.156:7077 --executor-memory 1G --total-executor-cores 1 /home/hadoop/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar spark://172.31.54.156:7077
 */
public class AddCartInfoGlobal {

    public static class SquareKey implements Function<Row, Integer> {
        public Integer call(Row row) throws Exception {
            System.out.println(row.toString());
            System.out.println(row.getInt(0)+"=========="+row.getString(1));
            return row.getInt(0) * row.getInt(0);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage AddCartInfoGlobal sparkMaster sql");
        }
        String master = args[0];
        String sql = args[1];
        System.out.println(System.getenv("SPARK_HOME")+":"+sql);
        JavaSparkContext sc = new JavaSparkContext(
                master, "AddCartInfoGlobal", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        HiveContext sqlContext = new HiveContext(sc.sc());
        HiveConf hiveConf = sqlContext.hiveconf();
        hiveConf.setBoolean("hive.map.aggr",true);
        hiveConf.set("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
        hiveConf.setBoolean("hive.exec.parallel",true);
        hiveConf.setInt("hive.exec.parallel.thread.number",2);
        hiveConf.setInt("mapred.reduce.tasks",1);
        DataFrame rdd = sqlContext.sql(sql);
        List<Row> rdds = rdd.collectAsList();
        for (Row row: rdds) {
            System.out.println(row.toString());
        }
    }
}
