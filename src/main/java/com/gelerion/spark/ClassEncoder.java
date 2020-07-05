package com.gelerion.spark;

import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class ClassEncoder {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("af-so")
                .master("local[1]")
                .getOrCreate();

        spark.range(10)
                .select(col("abc"));
//        spark.createDataset(Seq(Person("abcd", 10))).show()


    }

    static class Person {
        public String name;
        public int age;
        public Person (String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
