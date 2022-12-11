package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.coalesce;

public class WorkingWithNull {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataset = getDataSet(sparkSession);
        dataset.show();
        //coalesceExample(dataset);
        //dropNullRows(dataset);
        //fillNulls(dataset);
        replaceExample(dataset);
    }

    private static void replaceExample(Dataset<Row> dataset) {
        
        Map<Object,Object > objectMap = new HashMap<>();
        objectMap.put(90, -200);
        dataset.na().replace("Day1", objectMap).show();
    }

    private static void fillNulls(Dataset<Row> dataset) {
        dataset.na().fill(-1).show();
        dataset.na().fill(-2, new String[]{"Day1", "Day2"}).show();
        Map<String, Object> stringMap = new HashMap<>();
        stringMap.put("Day3", -5);
        stringMap.put("Day4", -10);
        dataset.na().fill(stringMap).show();
    }

    private static void dropNullRows(Dataset<Row> dataset) {
        dataset.na().drop().show(5);
        dataset.na().drop("any").show(5);
        dataset.na().drop("all").show(5);
        dataset.na().drop("any", new String[]{"Day1", "Day2"}).show(5);
    }

    private static void coalesceExample(Dataset<Row> dataset) {
        dataset.withColumn("StartingScore",
                coalesce(new Column("Day1"),
                        new Column("Day2"),
                        new Column("Day3"),
                        new Column("Day4"),
                        new Column("Day5")
                )).show(5);
    }

    private static Dataset<Row> getDataSet(SparkSession sparkSession) {
        return sparkSession.createDataFrame(getRows(), getSchema());
    }
    private static StructType getSchema(){
        StructType structType = new StructType(new StructField[]{
                DataTypes.createStructField("Player", DataTypes.StringType, false),
                DataTypes.createStructField("Day1", DataTypes.IntegerType, true),
                DataTypes.createStructField("Day2", DataTypes.IntegerType, true),
                DataTypes.createStructField("Day3", DataTypes.IntegerType, true),
                DataTypes.createStructField("Day4", DataTypes.IntegerType, true),
                DataTypes.createStructField("Day5", DataTypes.IntegerType, true)
        });
        return structType;
    }
    private static List<Row> getRows(){
        List<Row> rowList = new ArrayList<>();
        rowList.add(RowFactory.create("Sachin", null, null, 100, null, 150));
        rowList.add(RowFactory.create("Dravid", null, null, null, 30, 70));
        rowList.add(RowFactory.create("Ponting", 90, 10, null, null, 150));
        rowList.add(RowFactory.create("Gilchrist", 70, 10, null, null, 150));
        rowList.add(RowFactory.create("Sehwag", null, null, 100, 20, 150));
        return rowList;
    }
}
