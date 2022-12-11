package com.abhishyam.learn.select;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.abhishyam.learn.datasets.CreateDataSet.createDataSetOnFlyForMarvel;
import static org.apache.spark.sql.functions.expr;

//select allow you to do the DataFrame equivalent of SQL queries on a table of data:
public class SelectExamples {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataset = createDataSetOnFlyForMarvel(sparkSession);
        dataset.select("Hero").show();
        dataset.select("movie","Hero").show();
        dataset.select(new Column("Hero")).show();
        dataset.select(expr("Hero as abc")).show();
    }
}
