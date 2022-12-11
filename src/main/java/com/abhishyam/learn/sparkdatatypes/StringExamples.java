package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class StringExamples {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        Dataset<Row> dataset = BooleansExamples.readData(sparkSession);
       // initcapExample(dataset);
        //caseChange(dataset);
        //padding(dataset);

    }

    private static void padding(Dataset<Row> dataset) {
        dataset.select(
                ltrim(lit("       Hello          ")).as("Left Trim"),
                rtrim(lit("       Hello          ")).as("Right Trim"),
                trim(lit("        Hello          ")).as("Trim"),
                lpad(lit("1"), 3, "0").as("Left Pad"),
                rpad(lit("2."), 4, "0").as("Right Pad")
                )
                .show(2, false);
    }

    private static void caseChange(Dataset<Row> dataset) {
        // Covert to lower case
        dataset.select(lower(new Column("Description")).as("Lower"),
                new Column("Description"))
                .show(10, false);
    }

    /**
     *The initcap function will capitalize every word in a given string
     * when that word is separated from another by a space.
     */
    private static void initcapExample(Dataset<Row> dataset) {
        dataset.select(initcap(new Column("Description")).as("New Column"),
                        new Column("Description"))
                .show(10, false);
    }
}
