package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.column;

public class BooleansExamples {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();

        Dataset<Row> df = readData(sparkSession);
        df.printSchema();
        df.show();

       /* df.where(col("InvoiceNo").equalTo(536365))
                .select("InvoiceNo", "Description")
                .show(5, false);*/

        //andOrExample(df);

        addingColumn(df);
    }

    private static void addingColumn(Dataset<Row> df) {
        Column dotFilter = column("StockCode").notEqual("DOT");
        Column unitPriceFilter = column("UnitPrice").$greater(600);
        Column descriptionFilter = column("Description").contains("POSTAGE");

        df.withColumn("isExpensive", dotFilter.and(unitPriceFilter.or(descriptionFilter)))
                .where("isExpensive")
                .show(10);

    }

    private static void andOrExample(Dataset<Row> df) {
        Column unitPriceFilter = column("UnitPrice").$greater(600);
        Column descriptionFilter = column("Description").contains("POSTAGE");

        df.where(
                col("StockCode").equalTo("DOT")
        )
                .where(unitPriceFilter.or(descriptionFilter))
                .show(10);
    }

    public static Dataset<Row> readData(SparkSession sparkSession) {
        return sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("/Users/300069965/Documents/spark/data/retail-data/by-day/2010-12-01.csv");
    }
}
