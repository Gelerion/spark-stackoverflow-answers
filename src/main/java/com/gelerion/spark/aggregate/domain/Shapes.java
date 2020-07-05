package com.gelerion.spark.aggregate.domain;

import java.io.Serializable;
import java.util.Arrays;

public class Shapes implements Serializable {

    private static final long serialVersionUID = -8018523772473481858L;

    private Shape[] shapes;

    public Shape[] getShapes() {
        return shapes;
    }

    public void setShapes(Shape[] shapes) {
        this.shapes = shapes;
    }

    @Override
    public String toString() {
        return "Shapes [shapes=" + Arrays.toString(shapes) + "]";
    }
}
