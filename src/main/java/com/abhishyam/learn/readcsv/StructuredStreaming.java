package com.abhishyam.learn.readcsv;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class StructuredStreaming {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        StructType staticSchema = windowExample(sparkSession);
        Dataset<Row> streamingDS = sparkSession.readStream()
                .schema(staticSchema)
                .option("maxFilesPerTrigger", 1)
                .format("csv")
                .option("header", "true")
                .load("/Users/300069965/Documents/spark/data/retail-data/by-day/*.csv");
        System.out.println(streamingDS.isStreaming());

        Dataset<Row> purchaseByCustomerPerHour = streamingDS
                .selectExpr("CustomerID",
                        "(Quantity * UnitPrice) as Total_Cost",
                        "InvoiceDate"
                ).groupBy(
                        col("CustomerId"),
                        window(col("InvoiceDate"), "1 day")
                ).sum("Total_Cost")
                .sort(desc("sum(Total_Cost)"));

        purchaseByCustomerPerHour.writeStream()
                .format("memory")
                .queryName("customer_purchases")
                .outputMode("complete")
                .start();
    }



    private static StructType windowExample(SparkSession sparkSession){
        Dataset<Row> staticDS = sparkSession
                .read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("/Users/300069965/Documents/spark/data/retail-data/by-day/*.csv");

        staticDS.createOrReplaceTempView("retail_data");
        StructType staticSchema = staticDS.schema();

        staticDS
                .selectExpr("CustomerID",
                        "(Quantity * UnitPrice) as Total_Cost",
                        "InvoiceDate")
                .groupBy(col("CustomerID"),
                        window(col("InvoiceDate"), "1 day"))
                .sum("Total_Cost")
                .sort(desc("sum(Total_Cost)"))
                .show(5);

        return staticSchema;
    }
}
