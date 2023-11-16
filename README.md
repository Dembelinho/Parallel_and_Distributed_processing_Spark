# Parallel_and_Distributed_processing_Spark


![Exer 1](https://github.com/Dembelinho/Parallel_and_Distributed_processing_Spark/assets/110602716/6ebaffe8-59d9-4665-8a0b-616cdc3c7b0a)

```
        SparkConf conf = new SparkConf().setAppName("RDDLineage").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
```
Ce code illustre l'utilisation de l'API _org.apache.spark_ 

- On crée une liste d'étudiants nommés "studentNames".
  ``` List<String> studentNames = Arrays.asList("John Doe", "Jane Doe", "Bob Smith", "Alice Johnson");```

- On utilise la méthode "parallelize" pour créer un JavaRDD (Resilient Distributed Dataset) à partir de cette liste. 
``` JavaRDD<String> rdd1 = sc.parallelize(studentNames);```

- La méthode "flatMap" est utilisée pour diviser chaque nom en un tableau de mots.
- Ensuite, la méthode "iterator" est utilisée pour transformer le tableau en un objet Iterator.
``` JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator()); ```

- La méthode "filter" est utilisée pour garder seulement les mots qui commencent par "J".
  ```  JavaRDD<String> rdd3 = rdd2.filter(s -> s.startsWith("J")); ```
  
- La méthode "mapToPair" est utilisée pour créer un nouveau RDD où chaque mot est associé à 1.
``` JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(s -> new Tuple2<>(s, 1)); ```

- La méthode "reduceByKey" est utilisée pour additionner les nombres associés à chaque mot.
``` JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey((x, y) -> x + y); ```

- La méthode "mapToPair" est à nouveau utilisée pour créer un nouveau RDD où chaque paire (nombre, mot) est inversée.
 ``` JavaPairRDD<Integer, String> rdd6 = rdd5.mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1)); ```

- La méthode "sortByKey" est utilisée pour trier le RDD par clé (nombre) dans l'ordre décroissant.
   ```  JavaPairRDD<Integer, String> rdd7 = rdd6.sortByKey(false); ```
  
- Afficher le résultat

La méthode "collect" est utilisée pour récupérer tous les éléments du RDD en tant que liste locale.
```   System.out.println("Final RDD: " + rdd7.collect()); ```

- Fermer le SparkContext

Enfin, on ferme le SparkContext avec la méthode "close".
```  sc.close(); ```

## Exercice 2 :
1. On souhaite développer une application Spark permettant, à partir d’un fichier texte (ventes.txt) en entré, contenant les ventes d’une entreprise dans les différentes villes, de déterminer le total des ventes par ville.
La structure du fichier ventes.txt est de la forme suivante :
**date ville produit prix**

Vous testez votre code en local avant de lancer le job sur le cluster.

2. Vous créez une deuxième application permettant de calculer le prix total des ventes des produits par ville pour une année donnée.
