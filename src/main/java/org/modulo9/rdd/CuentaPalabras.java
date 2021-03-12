package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CuentaPalabras {
    public static void main(String[] args) {
        //Step 1. Create a SparkConf objectnumbers.txt
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add number")
                .setMaster("local[8]");
        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Step 3. Read the files content
        JavaRDD<String> lines = sparkContext.textFile("data/quijote.txt");
        /*
        //number of lines of the file
        long numLines = lines.count();
        System.out.println(numLines);
         */

        //preproces words, te las pone planas para poder contarlas, ponemos un espacio como separador
        JavaRDD<String> words = lines.flatMap(line -> List.of(line.split("")).iterator());

        // en este punto ya tengo un array de palabras ahora las tengo que contar

        JavaPairRDD<String,Integer> pares = words.mapToPair(word -> new Tuple2<>(word,1));//primero pasamos las palabras a clave valor

        //ahora contamos las palabras distintas

        JavaPairRDD<String,Integer> groupedPairs = pares.reduceByKey((integer1,integer2)-> integer1+integer2);

        //aqui ya tengo una lista que cuenta cuantas palabras hay de cada tipo de palabra que aparece

        List<Tuple2<String,Integer>> listapalabras = groupedPairs.collect();

        for(Tuple2<?, ?> tuple : listapalabras){
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        //Step 7. Stop Spark context
        sparkContext.stop();
    }
}
