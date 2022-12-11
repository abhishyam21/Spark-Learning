package com.abhishyam.learn.datasets;

import com.abhishyam.learn.SparkContextFactory;
import com.abhishyam.learn.utils.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class CreateDataSet {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        createDatasetFromFile(sparkSession);
        createDataSetOnFlyForMarvel(sparkSession);
    }

    public static Dataset<Row> createDataSetOnFlyForMarvel(SparkSession sparkSession) {
        List<Row> rows = getMarvelDataAsRows();
        StructType structType = getSchema();
        Dataset<Row> dataset = sparkSession.createDataFrame(rows, structType);
       // dataset.takeAsList(10).forEach(System.out::println);
        dataset.show();
        return dataset;
    }

    public static Dataset<Row> createDataSetOnFlyForDC(SparkSession sparkSession) {
        List<Row> rows = getDCDataAsRows();
        StructType structType = getSchema();
        Dataset<Row> dataset = sparkSession.createDataFrame(rows, structType);
        // dataset.takeAsList(10).forEach(System.out::println);
        dataset.show();
        return dataset;
    }

    public static Dataset<Row> createDatasetFromFile(SparkSession sparkSession) {
        Dataset<Row> ds = sparkSession
                .read()
                .format("json")
                .load(FileUtils.SUMMARY_2015);
        ds.show();
        return ds;
    }

    private static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movie", DataTypes.StringType, true),
                DataTypes.createStructField("Hero", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });
    }

    private static List<Row> getMarvelDataAsRows() {
        Row thor = RowFactory.create("Thor", "Chris Hemsworth", 39);
        Row ironMan = RowFactory.create("Iron Man", "robert downey jr", 57);
        Row ca = RowFactory.create("Captain America", "Chris Evans", 41);
        Row bw = RowFactory.create("Black Widow", "Scarlett Johansson", 38);
        return Arrays.asList(thor,ironMan,ca, bw);
    }

    private static List<Row> getDCDataAsRows() {
        Row aq = RowFactory.create("Aquaman", "Jason Momoa", 43);
        Row bat = RowFactory.create("Batman", "Christian Bale", 48);
        Row sm = RowFactory.create("superman", "Henry Cavill", 39);
        Row ww = RowFactory.create("Wonder Woman", "Gal Gadot", 37);
        return Arrays.asList(aq,bat,sm, ww);
    }
}
