package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class Criticality implements Serializable {

    private int critical;
    private int total;

    public Criticality() {
    }

    public Criticality(int critical, int total) {
        this.critical = critical;
        this.total = total;
    }

    public Criticality sum(Criticality addend) {
        return new Criticality(this.critical + addend.critical, this.total + addend.total);
    }

    public double CriticalPercent() {
        return ((double) critical / (double) total) * 100;
    }

}
