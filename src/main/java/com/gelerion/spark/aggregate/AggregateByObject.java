package com.gelerion.spark.aggregate;

import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AggregateByObject {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("af-so")
                .master("local[1]")
                .getOrCreate();

//        spark.createDataset()

    }

}
