package org.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        SparkConf conf= new SparkConf().setAppName("TP1 RDD").setMaster("local[*]");
        JavaSparkContext sparkContext=new JavaSparkContext(conf);
        JavaRDD<Integer> integerJavaRDD1=sparkContext.parallelize(Arrays.asList(12,17,19,20,15,85,501,4415));
        JavaRDD<Integer> integerJavaRDD2=integerJavaRDD1.map(a -> a+1); //transformation
        JavaRDD<Integer> rdd3 = integerJavaRDD2.filter(a -> a >= 20); //transformation
        List<Integer> integers = rdd3.collect(); //action
        for (Integer i: integers
             ) {
            System.out.println(i);
        }
    }
}