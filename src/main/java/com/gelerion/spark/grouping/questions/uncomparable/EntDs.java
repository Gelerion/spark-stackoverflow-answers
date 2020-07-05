package com.gelerion.spark.grouping.questions.uncomparable;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Function1;
import scala.Serializable;
import scala.Tuple2;
import org.apache.spark.sql.expressions.scalalang.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class EntDs {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("a")
                .master("local[1]")
//                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .config("spark.kryo.registrator", MyKryoRegistrator.class.getName())
                .getOrCreate();

        List<Entreprise> ent = new ArrayList<>();
        Entreprise e = new Entreprise("sigle1", "abc", "run");
        e.etablissements.put("abd", new Etablissement("abd", "dogle5"));
        ent.add(e);
        ent.add(new Entreprise("sigle2", "abc", "run"));
        ent.add(new Entreprise("sigle3", "abc", "run"));

        ArrayList<Etablissement> etab = new ArrayList<>();
        etab.add(new Etablissement("abc", "sigle1"));
        etab.add(new Etablissement("abc", "sigle2"));
        etab.add(new Etablissement("abc", "sigle3"));


        Dataset<Entreprise> dsEntreprises = spark.createDataset(ent, Encoders.bean(Entreprise.class));
        Dataset<Etablissement> dsEtablissements = spark.createDataset(etab, Encoders.bean(Etablissement.class));

        Dataset<Tuple2<Entreprise, Etablissement>> ds = dsEntreprises
                .joinWith(dsEtablissements, dsEntreprises.col("sigle").equalTo(dsEtablissements.col("sigle")), "inner");

//        ds.show();

        Dataset<Row> df = ds.toDF();

//        Grouper grouper = new Grouper();
        Dataset<Entreprise> grouped =  new Grouper().group(ds);

//        grouped.explain(true);
        grouped.show();
//        groupByKey.count().show();


    }

    public static class Grouper {

        Dataset<Entreprise> group(Dataset<Tuple2<Entreprise, Etablissement>> ds) {

            KeyValueGroupedDataset<Entreprise, Tuple2<Entreprise, Etablissement>> group = ds.groupByKey((MapFunction<Tuple2<Entreprise, Etablissement>, Entreprise>) f -> {
                Entreprise entreprise = f._1();
                Etablissement etablissement = f._2();
//                entreprise.ajouterEtablissement(etablissement);
                Entreprise res = new Entreprise(entreprise.getSigle(), entreprise.getNomNaissance(), entreprise.getNomUsage());
                entreprise.getEtablissements().values().forEach(res::ajouterEtablissement);
                res.ajouterEtablissement(etablissement);

                return res;
            }, Encoders.bean(Entreprise.class));

//            group.agg(typed.sum())

//            return group.keys();
            return group.mapGroups(new MapGroupsFunction<Entreprise, Tuple2<Entreprise, Etablissement>, Entreprise>() {
                                    @Override
                                    public Entreprise call(Entreprise key, Iterator<Tuple2<Entreprise, Etablissement>> values) {
                                        while(values.hasNext()) {
                                            Etablissement etablissement = values.next()._2();
                                            key.ajouterEtablissement(etablissement);
                                        }

                                        return key;
                                    }
                                },
                    Encoders.bean(Entreprise.class));
        }

    }


    public StructType schemaEntreprise() {
        StructType schema = new StructType()
                .add("sigle", StringType, true)
                .add("nomNaissance", StringType, true)
                .add("nomUsage", StringType, true);

        // Ajouter au Dataset des entreprises la liaison avec les établissements.
        MapType mapEtablissements = new MapType(StringType, schemaEtablissement(), true);
        StructField etablissements = new StructField("etablissements", mapEtablissements, true, Metadata.empty());
        schema.add(etablissements);

        return schema;
    }

    private DataType schemaEtablissement() {
        return new StructType()
                .add("name", StringType, true)
                .add("sigle", StringType, true);
    }

    public static class MyKryoRegistrator implements KryoRegistrator, Serializable {
        @Override
        public void registerClasses(Kryo kryo) {
            // Product POJO associated to a product Row from the DataFrame
            kryo.register(Entreprise.class);
            kryo.register(Etablissement.class);
        }
    }

}
