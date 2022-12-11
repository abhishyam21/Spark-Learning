package com.abhishyam.learn.schema;

import com.abhishyam.learn.SparkContextFactory;
import com.abhishyam.learn.model.Flight;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class CustomSchema {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();

        Dataset<Row> ds = sparkSession.read()
                .format("json")
                .schema(getManualSchema())
                .load("/Users/300069965/Documents/spark/data/flight-data/json/2015-summary.json");
        System.out.println(ds.schema());
        ds.takeAsList(5).forEach(System.out::println);
    }
    private static StructType getManualSchema(){
        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("DEST_COUNTRY_NAME", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("count", DataTypes.LongType, false));
        return DataTypes.createStructType(structFieldList);
    }
}
