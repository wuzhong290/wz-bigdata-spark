package com.yoho.recom.yohobuy.basic;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

/**
 * Created by dell on 2016/12/21.
 */
public class AddCartInfoGlobalSpark {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage AddCartInfoGlobalSpark sparkMaster csvOutputFile endDateDiff beginDateDiff");
        }
        String master = args[0];
        String outputFile = args[1];
//        String dateDiff = args[2];
//        Date now = new Date();
//        Date yesterday = DateUtils.addDays(now,-1);
//        String dateId = DateFormatUtils.format(DateUtils.addDays(yesterday,Integer.parseInt(dateDiff)),"yyyyMMdd");

        JavaSparkContext sc = new JavaSparkContext(
                master, "AddCartInfoGlobalSpark", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        //删除输出文件
        Configuration hadoopConf = sc.hadoopConfiguration();
        FileSystem hdfs = FileSystem.get(hadoopConf);
        Path path = new Path(outputFile);
        if(hdfs.exists(path)){
            try {
                hdfs.delete(path,true);
            }catch (Exception e){
                e.printStackTrace();
                return;
            }
        }
        HiveContext sqlContext = new HiveContext(sc.sc());
//        String sql1 = "select a.udid as browser_uniq_id,"
//                + "a.uid as user_log_acct,"
//                +"cast(get_json_object(a.param,'$.PRD_SKC') as int) as product_skc,"
//                +"substr(a.time_stamp, 0, 10) as time_stamp_click "
//                +"from fact.fact_mobile_click as a "
//                +"where concat(year, month, day) = " +dateId
//                +" and oper_id='YB_GLOBAL_GDS_DT_BUY'";
        String sql1 = "select * from clickinfo";
        DataFrame rdd = sqlContext.sql(sql1);
        //1、分组
        JavaPairRDD<Tuple2,Object[]> firstRdd = rdd.toJavaRDD().mapToPair(new PairFunction<Row, Tuple2, Object[]>() {
            @Override
            public Tuple2<Tuple2, Object[]> call(Row row) throws Exception {
                Tuple2 key = new Tuple2(row.get(0), row.get(2));
                Object[] value = new Object[]{row.get(0),row.get(1),row.get(2),row.get(3)};
                System.out.println(key.toString() +":" +ArrayUtils.toString(value));
                return new Tuple2(key,value);
            }
        });

        JavaPairRDD<Tuple2,Iterable<Object[]>> secondRDD = firstRdd.groupByKey();
        //JavaPairRDD<Tuple2,Iterable<Object[]>> secondRDD = firstRdd.reduceByKey();
        JavaRDD <Object[]> rddResult = secondRDD.map(new Function<Tuple2<Tuple2,Iterable<Object[]>>, Object[]>() {
            @Override
            public Object[] call(Tuple2<Tuple2,Iterable<Object[]>> integerIterableTuple2) throws Exception {
                Iterable<Object[]> iter = integerIterableTuple2._2();
                Object[] result = null;
                for (Object[] item : iter) {
                    System.out.println(ArrayUtils.toString(result) +":" +ArrayUtils.toString(item));
                    if(null != result && null != result[1]){
                        if(null != item[1]){
                            if(Integer.parseInt(String.valueOf(result[1]))<Integer.parseInt(String.valueOf(item[1]))){
                                result[1] = item[1];
                            }
                        }
                    }
                    if(null != result && null != result[3]){
                        if(null != item[3]){
                            if(Integer.parseInt(String.valueOf(result[3]))>Integer.parseInt(String.valueOf(item[3]))){
                                result[3] = item[3];
                            }
                        }
                    }
                    if(null == result){
                        result = item;
                    }
                }
                System.out.println(StringUtils.join(result,","));
                return result;
            }
        });
        //2、和表skc_skn通过skc进行jion
        JavaPairRDD<Integer,Object[]> rddResultPair =rddResult.mapToPair(new PairFunction<Object[], Integer, Object[]>() {
            @Override
            public Tuple2<Integer, Object[]> call(Object[] value) throws Exception {
                System.out.println("rddResultPair:"+ArrayUtils.toString(value));
                return new Tuple2(Integer.parseInt(String.valueOf(value[2])),value);
            }
        });
        String sql2 = "select * from recom_yohobuy.skc_skn";
        DataFrame rdd2 = sqlContext.sql(sql2);
        JavaPairRDD<Integer,Object[]> firstRdd2 = rdd2.toJavaRDD().mapToPair(new PairFunction<Row, Integer, Object[]>() {
            @Override
            public Tuple2<Integer, Object[]> call(Row row) throws Exception {
                Object[] value = new Object[]{row.get(0),row.get(1)};
                System.out.println("firstRdd2:"+ArrayUtils.toString(value));
                return new Tuple2(row.get(0),value);
            }
        });
        JavaPairRDD<Integer,Tuple2<Object[], Object[]>> secondRDD2 = rddResultPair.join(firstRdd2);
        JavaRDD <Object[]> rdd2Result =  secondRDD2.map(new Function<Tuple2<Integer,Tuple2<Object[], Object[]>>, Object[]>() {
            @Override
            public Object[] call(Tuple2<Integer, Tuple2<Object[], Object[]>> v1) throws Exception {
                System.out.println("rdd2Result:"+ArrayUtils.toString(v1._1()) +":" +ArrayUtils.toString(v1._2()._1())+":" +ArrayUtils.toString(v1._2()._2()));
                Object[] value1 =v1._2()._1();
                Object[] value2 =v1._2()._2();
                value1[2] = value2[1];
                return value1;
            }
        });
        //3、和表skn_info通过skn进行jion
        String sql3 = "select * from recom_yohobuy.skn_info";
        DataFrame rdd3 = sqlContext.sql(sql3);
        JavaPairRDD<Integer,Object[]> rdd2ResultPair =rdd2Result.mapToPair(new PairFunction<Object[], Integer, Object[]>() {
            @Override
            public Tuple2<Integer, Object[]> call(Object[] value) throws Exception {
                System.out.println("rdd2ResultPair:"+ArrayUtils.toString(value));
                return new Tuple2(Integer.parseInt(String.valueOf(value[2])),value);
            }
        });
        JavaPairRDD<Integer,Object[]> firstRdd3 = rdd3.toJavaRDD().mapToPair(new PairFunction<Row, Integer, Object[]>() {
            @Override
            public Tuple2<Integer, Object[]> call(Row row) throws Exception {
                Object[] value = new Object[]{row.get(0),row.get(1),row.get(2),row.get(3)};
                System.out.println("firstRdd3:"+ArrayUtils.toString(value));
                return new Tuple2(row.get(0),value);
            }
        });
        JavaPairRDD<Integer,Tuple2<Object[], Object[]>> secondRDD3 = rdd2ResultPair.join(firstRdd3);
        JavaRDD <String> rdd3Result =  secondRDD3.map(new Function<Tuple2<Integer,Tuple2<Object[], Object[]>>, String>() {

            @Override
            public String call(Tuple2<Integer, Tuple2<Object[], Object[]>> v1) throws Exception {
                System.out.println(ArrayUtils.toString(v1._1()) +":" +ArrayUtils.toString(v1._2()._1())+":" +ArrayUtils.toString(v1._2()._2()));
                Object[] value1 =v1._2()._1();
                Object[] value2 =v1._2()._2();
                Object[] result = new Object[]{value1[0],value1[1],value1[2],value1[3],value2[1],value2[2],value2[3]};
                System.out.println(StringUtils.join(result,","));
                return StringUtils.join(result,",");
            }
        });
//        rdd3Result.repartition(1).saveAsTextFile(outputFile)
//        rdd3Result.coalesce(1).saveAsTextFile(outputFile)
//        -rw-r--r--   3 hdfs supergroup          0 2016-12-22 00:08 /data/output/_SUCCESS
//        -rw-r--r--   3 hdfs supergroup         70 2016-12-22 00:08 /data/output/part-00000
        //否则输出多个part-0000X
        rdd3Result.repartition(1).saveAsTextFile(outputFile);
    }
}
