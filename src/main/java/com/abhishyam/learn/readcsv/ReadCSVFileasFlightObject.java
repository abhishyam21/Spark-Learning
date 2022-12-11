package com.abhishyam.learn.readcsv;

import com.abhishyam.learn.SparkContextFactory;
import com.abhishyam.learn.model.Flight;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Predicate;

public class ReadCSVFileasFlightObject {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> datasetRow = sparkSession
                .read()
                .parquet("/Users/300069965/Documents/spark/data/flight-data/parquet/2010-summary.parquet/");
        Dataset<Flight> flightDS = datasetRow.as(Encoders.bean(Flight.class));
        flightDS.show();
        Predicate<Flight> canadaFlight = flight -> flight.getOrigin_country_name().equals("Canada");
    }
}
