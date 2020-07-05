package com.gelerion.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.*;

public class GeneralJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("af-so")
                .master("local[1]")
                .getOrCreate();


        List<Optional<String>> data = new ArrayList<>();

//        data.add()

        List<Column> columns = new ArrayList<>();
        columns.add(col("name"));
        columns.add(col("name1"));

        Column[] cols = {col("name")};
//        spark.createDataFrame()
        Dataset<Row> non = spark.range(10).select("a");
//                .select(coalesce(cols).as("non"));
    }

}
