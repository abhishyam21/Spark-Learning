package com.abhishyam.learn.schema;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class PrintSchema {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> ds = sparkSession.read()
                .format("json")
                .load("/Users/300069965/Documents/spark/data/flight-data/json/2015-summary.json");
        //Print the structure of the file
        ds.printSchema();

        // Get the Spark level schema for the input file
        StructType structType = sparkSession.read()
                .format("json")
                .load("/Users/300069965/Documents/spark/data/flight-data/json/2015-summary.json").schema();

        System.out.println(structType.toString());
        ds.takeAsList(5).forEach(System.out::println);
    }
}
