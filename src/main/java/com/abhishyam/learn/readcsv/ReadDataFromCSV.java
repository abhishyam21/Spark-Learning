package com.abhishyam.learn.readcsv;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.desc;

public class ReadDataFromCSV {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> flightData2015 = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("/Users/300069965/Documents/spark/data/flight-data/csv/2015-summary.csv");
        flightData2015.createOrReplaceTempView("flight_data_2015");
        Dataset<Row> sqlWay = sparkSession.sql("select DEST_COUNTRY_NAME, count(1) from flight_data_2015 " +
                "group by DEST_COUNTRY_NAME");
        Dataset<Row> datasetWay = flightData2015
                .groupBy("DEST_COUNTRY_NAME")
                .count();

        sparkSession.sql("select max(count) from flight_data_2015").show();
        sparkSession.sql("SELECT DEST_COUNTRY_NAME, sum(count) as destination_total\n" +
                "FROM flight_data_2015\n" +
                "GROUP BY DEST_COUNTRY_NAME\n" +
                "ORDER BY sum(count) DESC\n" +
                "LIMIT 5").show();
        flightData2015
                .groupBy("DEST_COUNTRY_NAME")
                .sum("count")
                .withColumnRenamed("sum(count)", "destination_total")
                .sort(desc("destination_total"))
                .limit(5)
                .show();
    }

}
