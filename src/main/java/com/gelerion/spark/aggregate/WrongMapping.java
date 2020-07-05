package com.gelerion.spark.aggregate;

import com.gelerion.spark.aggregate.domain.Shape;
import com.gelerion.spark.aggregate.domain.Shapes;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode;

import static org.apache.spark.sql.functions.*;


public class WrongMapping {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("af-so")
                .master("local[1]")
//                .config("spark.sql.codegen.wholeStage", false)
//                .config("spark.sql.codegen.factoryMode", CodegenObjectFactoryMode.NO_CODEGEN().toString())
                .getOrCreate();

        //Step 1 Read from input
        Dataset<Row> df = spark.read().format("json").option("header",  true).load("src/main/resources/shapes.json");
        //Step 2 Use bean encoder
        Dataset<Shapes> shapeDf = df.as(Encoders.bean(Shapes.class));
        shapeDf.show(); // This shows correct values

        //Step 3 Use map fucntion
        Dataset<Shapes> anotherShapeDf = shapeDf.map((MapFunction<Shapes, Shapes>) row -> {
            System.out.println(row); // Wrongly mapped values being printed
            return row;
        }, Encoders.bean(Shapes.class));

        // Wrong values are mapped
        anotherShapeDf.show();


        /*//        df.show();

//        Dataset<Shapes> shapeDf = df.map((MapFunction<Row, Shapes>) row -> {
//            double area = row.<Row>getList(0).get(0).getDouble(0);
//            boolean isRound = row.<Row>getList(0).get(0).getBoolean(1);
//            long length = row.<Row>getList(0).get(0).getLong(2);
//
//            Shape shape = new Shape();
//            shape.setLength(length);
//            shape.setArea(area);
//            shape.setRound(isRound);
//            Shape[] shapes = {shape};
//
//            Shapes result = new Shapes();
//            result.setShapes(shapes);
//            return result;
//        }, Encoders.bean(Shapes.class));
//
//        shapeDf.show(false);

//        Shape shape = new Shape();
//        shape.setLength(0L);
//        shape.setArea(73488.0);
//        shape.setRound(true);
//        Shape[] shapes = {shape};
//
//        Shapes result = new Shapes();
//        result.setShapes(shapes);
//
//        ExpressionEncoder<Shapes> encoder = (ExpressionEncoder<Shapes>) Encoders.bean(Shapes.class);
//        InternalRow internalRow = encoder.toRow(result);
//        System.out.println("internalRow = " + internalRow);

        //Step 2 Use bean encoder
        Dataset<Shapes> shapeDf = df.as(Encoders.bean(Shapes.class));
//        shapeDf.show(false); // This shows correct values
//        shapeDf.printSchema();
        //shapeDf.select(from_json(col("shapes"))).show();

        //Step 3 Use map fucntion
        Dataset<Shapes> anotherShapeDf = shapeDf.map((MapFunction<Shapes, Shapes>) row -> {
            System.out.println(row); // Wrongly mapped values being printed
            return row;
        }, Encoders.bean(Shapes.class));
//        anotherShapeDf.explain(true);

        // Wrong values are mapped
        anotherShapeDf.show(false);*/

    }

}
