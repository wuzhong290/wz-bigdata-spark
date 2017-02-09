package com.yoho.recom.yohobuy.demo;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
/**
 * Created by wuzhong on 2017/2/7.
 */
public class SparkMongodbJavaExample {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection");

        JavaSparkContext jsc = new JavaSparkContext(sc); // Create a Java Spark Context

        JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
                    public Document call(final Integer i) throws Exception {
                        return Document.parse("{test: " + i + "}");
                    }
                });

        MongoSpark.save(documents);


        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
                    public Document call(final Integer i) throws Exception {
                        return Document.parse("{spark: " + i + "}");
                    }
                });

        // Saving data from an RDD to MongoDB
        MongoSpark.save(sparkDocuments, writeConfig);


        // Loading and analyzing data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        System.out.println(rdd.count());
        System.out.println(rdd.first().toJson());

        // Loading data with a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "spark");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        JavaRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

        System.out.println(customRdd.count());
        System.out.println(customRdd.first().toJson());

        // Filtering an rdd using an aggregation pipeline before passing data to Spark
        JavaRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { test : { $gt : 5 } } }")));
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.first().toJson());

        // Saving data with a custom WriteConfig
        Map<String, String> writeOverrides1 = new HashMap<String, String>();
        writeOverrides1.put("collection", "myNewColl");
        WriteConfig writeConfig1 = WriteConfig.create(jsc).withOptions(writeOverrides1);

        List<String> characters = asList(
                "{'name': 'Bilbo Baggins', 'age': 50}",
                "{'name': 'Gandalf', 'age': 1000}",
                "{'name': 'Thorin', 'age': 195}",
                "{'name': 'Balin', 'age': 178}",
                "{'name': 'Kíli', 'age': 77}",
                "{'name': 'Dwalin', 'age': 169}",
                "{'name': 'Óin', 'age': 167}",
                "{'name': 'Glóin', 'age': 158}",
                "{'name': 'Fíli', 'age': 82}",
                "{'name': 'Bombur'}"
        );
        MongoSpark.save(jsc.parallelize(characters).map(new Function<String, Document>() {
            public Document call(final String json) throws Exception {
                return Document.parse(json);
            }
        }), writeConfig1);

        SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
        DataFrame df = MongoSpark.load(jsc).toDF();
        df.printSchema();

        DataFrame centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age >= 100");

        Encoder<Map> encoder = Encoders.bean(Map.class);
        MongoSpark.write(centenarians.as(encoder)).option("collection", "hundredClub").mode("overwrite").save();

        // Load the data from the "hundredClub" collection
        MongoSpark.load(sqlContext, ReadConfig.create(sqlContext).withOption("collection", "hundredClub"), Character.class).show();
    }
}
