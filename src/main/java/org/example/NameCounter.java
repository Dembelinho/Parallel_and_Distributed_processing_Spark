package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class NameCounter {
    public static void main(String[] args) {
        SparkConf conf= new SparkConf().setAppName("Counter").setMaster("local[*]");
        JavaSparkContext sparkContext=new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD1 = sparkContext.textFile("names.txt");
        JavaRDD<String> JavaRDD2 = stringJavaRDD1.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
        JavaPairRDD<String, Integer> javaPairRDD3 = JavaRDD2.mapToPair(mot -> new Tuple2<>(mot, 1));
        JavaPairRDD<String, Integer> rdd4 = javaPairRDD3.reduceByKey((a, z) -> {
            System.out.println("a -->" + a + " z-->" + z);
            return a + z;
        });
        rdd4.foreach(stringIntegerTuple2 -> {
            System.out.println(stringIntegerTuple2._1+"  "+stringIntegerTuple2._2);
        });
    }
}
