package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class ComplexTypes {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataset = BooleansExamples.readData(sparkSession);
        //structsExamples(dataset);
        arraysExample(dataset);
        mapExamples(sparkSession);
    }

    private static void mapExamples(SparkSession sparkSession) {
        Dataset<Row> dataset = createMap(sparkSession);
        dataset.show(false);
        queryMap(dataset);
        explodeExample(dataset);
    }

    private static void explodeExample(Dataset<Row> dataset) {
        dataset.select(explode(new Column("complex_map")))
                .show(false);
    }

    private static void queryMap(Dataset<Row> dataset) {
        dataset.selectExpr("complex_map['Batman']")
                .show(2,false);
    }

    private static Dataset<Row> createMap(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.createDataFrame(getData(), getSchema());
        return dataset.select(map(new Column("Character"), new Column("Hero"))
                .as("complex_map"));
    }

    private static void arraysExample(Dataset<Row> dataset) {
        Dataset<Row> arrayDataSet = createArray(dataset);
        // queryArray(arrayDataSet);
        //arraySize(arrayDataSet);
        //explodeExample();
    }

    private static void explodeExample() {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataFrame = sparkSession.createDataFrame(getData(), getSchema());
        dataFrame.show(10, false);
        //Explode will not consider the row with  null values
        dataFrame.withColumn("Movie",explode(split(new Column("Series"), ",")))
                .drop("Series")
                .show(30, false);
        // Will include null rows also
        dataFrame.withColumn("Movie",explode_outer(split(new Column("Series"), ",")))
                .drop("Series")
                .show(30, false);

        //Will provide the position of the element
        dataFrame.select(posexplode(split(new Column("Series"), ",")))
                .show(30, false);
        dataFrame.select(posexplode_outer(split(new Column("Series"), ",")))
                .show(30, false);
    }

    private static void arraySize(Dataset<Row> arrayDataSet) {
        arrayDataSet.select(size(new Column("Split")))
                .show(10, false);
    }

    private static void queryArray(Dataset<Row> arrayDataSet) {
        arrayDataSet.selectExpr("Split[1]", "*")
                .show(10, false);
        arrayDataSet.select(array_contains(new Column("Split"), "WHITE"))
                .show(10, false);
    }

    private static Dataset<Row> createArray(Dataset<Row> dataset) {
        return dataset.select(split(new Column("Description"), " ").as("Split"));
    }

    /**
     * You can think of structs as DataFrames within DataFrames.
     *
     */
    private static void structsExamples(Dataset<Row> dataset) {
        // createStruct(dataset);
        //queryStruct(dataset);

    }

    private static void queryStruct(Dataset<Row> dataset) {
        Dataset<Row> complexDs = dataset.select(struct("Description", "InvoiceNo").as("complex"));
        complexDs.show(10, false);
        complexDs.select("complex.Description").show(5, false);
        complexDs.select(new Column("complex").getField("InvoiceNo")
                .as("InvoiceNo"))
                .show(5, false);
        complexDs.select("complex.*").show(10, false);
    }

    private static void createStruct(Dataset<Row> dataset) {
        dataset.selectExpr("(Description, InvoiceNo) as complex", "*")
                .show(10,false);
        dataset.selectExpr("struct(Description, InvoiceNo) as struct", "*")
                .show(10,false);
    }

    public static StructType getSchema(){
        return  new StructType(new StructField[]{
                DataTypes.createStructField("Character", DataTypes.StringType, false),
                DataTypes.createStructField("Series", DataTypes.StringType, true),
                DataTypes.createStructField("Hero", DataTypes.StringType, false),
        });
    }
    public static List<Row> getData(){

        List<Row> rowList = new ArrayList<>();
        rowList.add(RowFactory.create("IronMan", "Iron Man 1, Iron Man 2, Iron Man 3", "Robert Downey Jr."));
        rowList.add(RowFactory.create("Batman", "Batman Begins, The Dark Knight, The Dark Knight Rises", "Christian Bale"));
        rowList.add(RowFactory.create("Captain America", "Captain America: First Avenger, Captain America: The Winter Soldier, Captain America: Civil War", "Chris Evans"));
        rowList.add(RowFactory.create("Winter soldier", null ,"Sebastian Stan"));
        return rowList;
    }
}
