package com.gelerion.spark.aggregate.domain;

import org.apache.spark.sql.Row;

public class AgreementLine{
    public String Agreement_A1;
    public String agreementCrocCode;
    public Line line;



    public static AgreementLine apply(Row row) {
        AgreementLine agrLine = new AgreementLine();
        agrLine.Agreement_A1 = row.getAs("Agreement_A1");
        Line res = new Line();
        res.Line_A1 = row.getAs("Line_A1");
        res.Line_A2 = row.getAs("Line_A2");
        agrLine.line = res;
        return agrLine;
    }
}
