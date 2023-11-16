package org.example.Exercice1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkRDDLineage {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RDDLineage").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> studentNames = Arrays.asList("John Doe", "Jane Doe", "Bob Smith", "Alice Johnson");

        JavaRDD<String> rdd1 = sc.parallelize(studentNames);

        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaRDD<String> rdd3 = rdd2.filter(s -> s.startsWith("J"));

        JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Integer, String> rdd6 = rdd5.mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1));

        JavaPairRDD<Integer, String> rdd7 = rdd6.sortByKey(false);

        System.out.println("Final RDD: " + rdd7.collect());

        sc.close();
    }
}
