package com.yoho.recom.yohobuy.demo;

/**
 * Created by dell on 2017/1/10.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.Array;
import scala.Tuple2;

/**
 * Example using MLlib ALS from Java.
 */
public final class JavaALS {

    static class ParseRating implements Function<String, Rating> {
        private static final Pattern COMMA = Pattern.compile("::");

        @Override
        public Rating call(String line) {
            String[] tok = COMMA.split(line);
            int x = Integer.parseInt(tok[0]);
            int y = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);
            return new Rating(x, y, rating);
        }
    }

    static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
        @Override
        public String call(Tuple2<Object, double[]> element) {
            return element._1() + "," + Arrays.toString(element._2());
        }
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\tools\\winutils");
        if (args.length < 3) {
            System.err.println(
                    "Usage: JavaALS <ratings_file> <rank> <iterations> <output_dir> [<blocks>]");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local[2]");
        int rank = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        //String outputDir = args[3];
        int blocks = -1;
        if (args.length == 5) {
            blocks = Integer.parseInt(args[4]);
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<Rating> ratings = lines.map(new ParseRating());

        MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);

        List<String> userFeatures = model.userFeatures().toJavaRDD().map(new FeaturesToString()).collect();
        for (String feature : userFeatures){
            System.out.println(feature);
        }
        System.out.println("=======================");
        List<String> productFeatures = model.productFeatures().toJavaRDD().map(new FeaturesToString()).collect();
        for (String feature : productFeatures){
            System.out.println(feature);
        }
        for(Rating rating : model.recommendProducts(1,10)) {
            System.out.println(rating.toString());
        }

//        model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
//                outputDir + "/userFeatures");
//        model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
//                outputDir + "/productFeatures");
        //System.out.println("Final user/product features written to " + outputDir);

        sc.stop();
    }
}
