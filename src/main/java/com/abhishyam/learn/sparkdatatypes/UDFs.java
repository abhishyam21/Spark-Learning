package com.abhishyam.learn.sparkdatatypes;

import com.abhishyam.learn.SparkContextFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UDFs {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkContextFactory.getSparkSession();
        UDF1<Long, Long> pow3UDF = n -> n * n * n;
        sparkSession.udf().register("myUDF1", pow3UDF, DataTypes.LongType);

        sparkSession.range(11)
                .selectExpr("myUDF1(id)")
                .show();
    }
}
