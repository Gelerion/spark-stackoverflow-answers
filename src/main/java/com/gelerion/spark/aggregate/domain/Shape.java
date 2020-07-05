package com.gelerion.spark.aggregate.domain;

import java.io.Serializable;

public class Shape implements Serializable {

    private static final long serialVersionUID = 7293213441670072327L;

    private Long length;
    private Double area;
    private boolean isRound;

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Double getArea() {
        return area;
    }

    public void setArea(Double area) {
        this.area = area;
    }

    public boolean isRound() {
        return isRound;
    }

    public void setRound(boolean isRound) {
        this.isRound = isRound;
    }

    @Override
    public String toString() {
        return "Shape [length=" + length + ", area=" + area + ", isRound=" + isRound + "]";
    }

}
