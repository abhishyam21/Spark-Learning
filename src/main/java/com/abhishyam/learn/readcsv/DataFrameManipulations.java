package com.abhishyam.learn.readcsv;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.Metadata.empty;

import static org.apache.spark.sql.functions.*;

public class DataFrameManipulations {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();

        creatingDataFrameManually(sparkSession);

        Dataset<Row> jsonDS = sparkSession.read().format("json")
                .load("/Users/300069965/Documents/spark/data/flight-data/json/2015-summary.json");

        selectExamples(jsonDS);
        selectExprExamples(jsonDS);
        addingLiteralExamples(jsonDS);
        filterExamples(jsonDS);
        sampleExamples(jsonDS);
    }

    private static void sampleExamples(Dataset<Row> jsonDS) {
        jsonDS.sample(
                false,
                0.5
        ).count();
    }

    private static void filterExamples(Dataset<Row> jsonDS) {
        jsonDS.filter((FilterFunction<Row>) row -> row.getLong(2) < 5).show(5);
    }

    private static void addingLiteralExamples(Dataset<Row> jsonDS) {
        jsonDS.select(
                expr("*"),
                lit(col("count")).as("One")
                )
                .show(10);
    }

    private static void selectExprExamples(Dataset<Row> jsonDS) {
        jsonDS.selectExpr(
                "*",
                "(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as withinCountry"
        ).show(5);

        jsonDS.selectExpr(
                "avg(count)",
                "count(distinct(DEST_COUNTRY_NAME))"
        ).show(5);
    }

    private static void selectExamples(Dataset<Row> jsonDS) {
        jsonDS.select("DEST_COUNTRY_NAME").show(5);
        jsonDS.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(5);
        System.out.println("Different ways of selecting column");
        jsonDS.select(
                jsonDS.col("DEST_COUNTRY_NAME"),
                col("DEST_COUNTRY_NAME"),
                column("DEST_COUNTRY_NAME"),
                expr("DEST_COUNTRY_NAME")
        ).show(5);
    }

    private static void creatingDataFrameManually(SparkSession sparkSession) {
        StructType structType = new StructType(new StructField[]{
                new StructField("some", StringType, true, empty()),
                new StructField("col", StringType, true, empty()),
                new StructField("names", LongType, false, empty()),
        });

        List<Row> rows = Arrays.asList(RowFactory.create("Ram", "1", 1L),
                RowFactory.create("Kiran", "2", 2L) );

        Dataset<Row> dataFrame = sparkSession.createDataFrame(rows, structType);
        dataFrame.show(10);
    }
}
