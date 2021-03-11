package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AddNumbersFromFile {
    public static void main(String[] args) {
        //Step 1. Create a SparkConf objectnumbers.txt
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add number")
                .setMaster("local[4]");

        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Step 3. Read the files content
        JavaRDD<String> lines = sparkContext.textFile("data/numbers.txt");
        //Step 4. Get a RDD of integers
        JavaRDD<Integer> numbers = lines.map(line -> Integer.valueOf(line));
        //Step5. Sum the numbers
        int sum =numbers.reduce((number1, number2)-> number1 + number2);

        System.out.println("The sum is: " + sum);
        //Step 7. Stop Spark context
        sparkContext.stop();

    }
}
