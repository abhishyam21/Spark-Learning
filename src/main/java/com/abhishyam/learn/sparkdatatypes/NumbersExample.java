package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.abhishyam.learn.sparkdatatypes.BooleansExamples.readData;

import static org.apache.spark.sql.functions.*;

public class NumbersExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataset = readData(sparkSession);
        // addColumnWithCal(dataset);
       // roundOff(dataset);

    }

    private static void roundOff(Dataset<Row> dataset) {
        dataset.select(org.apache.spark.sql.functions.round(new Column("UnitPrice"), 1)
                        .as("Rounded"), new Column("UnitPrice"))
                .show();
        dataset.select(round(lit(2.5)), bround(lit(2.5))).show(2);
    }

    // SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
    //FROM dfTable
    private static void addColumnWithCal(Dataset<Row> dataset) {
        Column fabricatedQuantity = pow(
                column("Quantity")
                        .multiply(
                                column("UnitPrice")), 2)
                .plus(5);
        dataset.select(
                col("CustomerId"),
                fabricatedQuantity
                        .alias("realQuantity")
        ).show(10);

        dataset.selectExpr("CustomerId", "POWER((Quantity * UnitPrice), 2)+5 as realQuantity")
                .show(10);
    }
}
