package com.abhishyam.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkContextFactory {

    public static SparkSession getSparkSession() {
        SparkConf config = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SampleCsvProgram")
                .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
                .set("spark.sql.shuffle.partitions", "1");

        return SparkSession
                .builder()
                .config(config)
                .getOrCreate();
    }
}

