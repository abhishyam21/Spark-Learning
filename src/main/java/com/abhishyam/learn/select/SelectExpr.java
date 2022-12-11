package com.abhishyam.learn.select;

import com.abhishyam.learn.SparkContextFactory;
import com.abhishyam.learn.datasets.CreateDataSet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//This is used where we can define all the functions as string expressions.
// This is very powerful
public class SelectExpr {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataset = CreateDataSet.createDatasetFromFile(sparkSession);
        // printAllColumns(dataset);
        //   changeColumnName(dataset);
        //addWithInCountryFlag(dataset);
        // aggValues(dataset);
        // removeColumn(dataset);
        // changeColumnType(dataset);
        // filter(dataset);
        //distinctRows(dataset);
         // unionDS(sparkSession);
        // sorting(sparkSession);
        partition(dataset);
    }

    /*
    Repartition will incur a full shuffle of the data, regardless of whether one is necessary.
    This means that you should typically only repartition when
    the future number of partitions is greater than your current number of partitions
    or when you are looking to partition by a set of columns.
     */
    private static void partition(Dataset<Row> dataset) {
        System.out.println(dataset.rdd().getNumPartitions());
        Dataset<Row> repartitionDS = dataset.repartition(5);
        System.out.println(repartitionDS.rdd().getNumPartitions());
        Dataset<Row> columnRepartitionDs = dataset.repartition(10, new Column("DEST_COUNTRY_NAME"));
        System.out.println(columnRepartitionDs.rdd().getNumPartitions());
    }

    private static void sorting(SparkSession sparkSession) {
        Dataset<Row> marvelDS = CreateDataSet.createDataSetOnFlyForMarvel(sparkSession);
        Dataset<Row> dcDS = CreateDataSet.createDataSetOnFlyForDC(sparkSession);
        Dataset<Row> moviesDS = marvelDS.union(dcDS);
        moviesDS.sort("age").show();
        moviesDS.orderBy("movie").show();
        moviesDS.orderBy(new Column("movie").desc_nulls_first(), new Column("age").asc_nulls_last()).show();
    }

    private static void unionDS(SparkSession sparkSession) {
        Dataset<Row> marvelDS = CreateDataSet.createDataSetOnFlyForMarvel(sparkSession);
        Dataset<Row> dcDS = CreateDataSet.createDataSetOnFlyForDC(sparkSession);
        marvelDS.union(dcDS).show();
        marvelDS.union(dcDS)
                .where("age <= 40")
                .show();
    }

    private static void distinctRows(Dataset<Row> dataset) {
        long count = dataset.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
                .distinct()
                .count();
        System.out.println("Total distinct rows: "+ count);
    }

    private static void filter(Dataset<Row> dataset) {
        dataset.filter(new Column("count").$greater$eq(50)).show();
        dataset.where("count >= 50")
                .show();
        dataset.where("count >= 50")
                .where("DEST_COUNTRY_NAME = \"United States\"")
                .show();
    }

    private static void changeColumnType(Dataset<Row> dataset) {
        dataset.withColumn("count_2",
                        new Column("count").cast("long"))
                .show();
    }

    private static void removeColumn(Dataset<Row> dataset) {
        dataset.selectExpr("*")
                .drop("count")
                .show();
        dataset.selectExpr("*")
                .drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
                .show();
    }

    private static void aggValues(Dataset<Row> dataset) {
        dataset.selectExpr("count(distinct(DEST_COUNTRY_NAME)) as country_count", "avg(count)")
                .show();
    }

    private static void addWithInCountryFlag(Dataset<Row> dataset) {
        dataset.selectExpr("*",
                        "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
                .show();

        dataset.withColumn("withinCountry_new", org.apache.spark.sql.functions.expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME"))
                .show();
    }

    private static void changeColumnName(Dataset<Row> dataset) {
        dataset.selectExpr("DEST_COUNTRY_NAME as new_column", "DEST_COUNTRY_NAME")
                .show();

        dataset.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
                .show();
    }

    private static void printAllColumns(Dataset<Row> dataset) {
        dataset.selectExpr("*")
                .show();
    }
}
