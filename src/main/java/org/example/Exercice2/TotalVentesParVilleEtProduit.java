package org.example.Exercice2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TotalVentesParVilleEtProduit {

    public static void main(String[] args) {
        // Configuration Spark
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVilleEtProduit").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Chargement du fichier ventes.txt en tant que RDD
        JavaRDD<String> lines = sc.textFile("chemin/vers/ventes.txt");

        // Transformation du RDD pour calculer le total des ventes par ville et produit
        JavaRDD<String> totalVentesParVilleEtProduit = lines
                .filter(line -> line.startsWith("2023")) // Filtrer les ventes pour l'année 2023
                .mapToPair(line -> {
                    String[] parts = line.split(" ");
                    String ville = parts[1];
                    String produit = parts[2];
                    Double prix = Double.parseDouble(parts[3]);
                    return new Tuple2<>(ville + "_" + produit, prix);
                })
                .reduceByKey((prix1, prix2) -> prix1 + prix2)
                .map(tuple -> tuple._1 + " : " + tuple._2);

        // Affichage du résultat
        totalVentesParVilleEtProduit.foreach(System.out::println);

        // Arrêt du contexte Spark
        sc.stop();
    }
}

