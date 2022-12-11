package com.abhishyam.learn.readcsv;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class SparkTypes {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        // inferring schema from spark
        Dataset<Row> jsonDS = sparkSession.read().format("json")
                .load("/Users/300069965/Documents/spark/data/flight-data/json/2015-summary.json");
        jsonDS.printSchema();

        //manually defining the schema
        StructType manualSchema = new StructType(new StructField[]{
                new StructField("dest_country_name", DataTypes.StringType, true, null),
                new StructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true, null),
                new StructField("count", DataTypes.LongType, true, Metadata.fromJson("{\"hello\":\"world\"}"))
        });

        Dataset<Row> manualSchemaDS = sparkSession.read().format("json")
                .schema(manualSchema)
                .load("/Users/300069965/Documents/spark/data/flight-data/json/2015-summary.json");
        manualSchemaDS.printSchema();

    }
}
