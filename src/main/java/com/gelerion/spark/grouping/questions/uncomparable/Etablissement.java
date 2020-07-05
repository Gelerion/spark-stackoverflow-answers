package com.gelerion.spark.grouping.questions.uncomparable;

import java.io.Serializable;

public class Etablissement implements Serializable {

    public Etablissement() {
    }

    String name;
    String sigle;

    public Etablissement(String name, String sigle) {
        this.name = name;
        this.sigle = sigle;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSigle() {
        return sigle;
    }

    public void setSigle(String sigle) {
        this.sigle = sigle;
    }
}
