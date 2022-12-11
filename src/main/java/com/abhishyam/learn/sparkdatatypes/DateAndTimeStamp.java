package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DateAndTimeStamp {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        //Dataset<Row> dataset = currentDateAndTime(sparkSession);
        // addingAndSubtractDays(dataset);
        Dataset<Row> dataset = convertStringToDate(sparkSession);
        differenceInDays(dataset);
        differenceInMonths(dataset);
        dateFormat(dataset);
        comparingDates(dataset);
    }

    private static void comparingDates(Dataset<Row> dataset) {
        dataset.filter(new Column("Date-1").$greater$eq(new Column("Date-2")))
                .show(2);
    }

    private static void dateFormat(Dataset<Row> dataset) {
        dataset.withColumn("Date-1",lit("2022-09-23"))
                .withColumn("Date-2", lit("28-07-2021"))
                .select(to_date(new Column("Date-1"), "yyyy-MM-dd").as("Date1"),
                        to_date(new Column("Date-2"), "dd-MM-yyyy").as("Date2"),
                        // Even if the format is wrong it won't throw error.
                        to_date(new Column("Date-2"), "dd-MMM-YYYY").as("Date3"))
                .show(2);
    }

    private static void differenceInMonths(Dataset<Row> dataset) {
        dataset.withColumn("Today", current_date())
                .select(months_between(new Column("Date-1"), new Column("Today")))
                .show(2);
    }

    private static void differenceInDays(Dataset<Row> dataset) {
        dataset.select(datediff(new Column("Date-1"), new Column("Date-2")))
                .show();
    }

    private static Dataset<Row> convertStringToDate(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.range(5)
                .withColumn("Date-1", to_date(lit("2020-03-01")))
                .withColumn("Date-2", to_date(lit("2020-03-25")));
        dataset.show();
        return dataset;
    }

    private static void addingAndSubtractDays(Dataset<Row> dataset) {
        dataset.select(date_sub(new Column("today"), 5), date_add(new Column("today"), 10))
                .show(2, false);
    }

    private static Dataset<Row> currentDateAndTime(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.range(10)
                .withColumn("today", current_date())
                .withColumn("now", current_timestamp());
        dataset.printSchema();
        dataset.show(4, false);
        return dataset;
    }
}
