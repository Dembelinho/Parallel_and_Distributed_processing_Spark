package org.example.Exercice2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class TotalVentesParVille {

    public static void main(String[] args) {
        // Configuration Spark
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVille").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Chargement du fichier ventes.txt en tant que RDD
        JavaRDD<String> lines = sc.textFile("ventes.txt");

        // Transformation du RDD pour calculer le total des ventes par ville
        JavaRDD<String> totalVentesParVille = lines.mapToPair(line -> {
                    String[] parts = line.split(" ");
                    String ville = parts[1];
                    Double prix = Double.parseDouble(parts[3]);
                    return new Tuple2<>(ville, prix);
                }).reduceByKey((prix1, prix2) -> prix1 + prix2)
                .map(tuple -> tuple._1 + " : " + tuple._2);

        // Affichage du résultat
        totalVentesParVille.foreach(System.out::println);

        // Arrêt du contexte Spark
        sc.stop();
    }
}
