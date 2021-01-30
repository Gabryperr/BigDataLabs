package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class Misleading implements Serializable {

    private int mislead;
    private int total;

    public Misleading() {
    }

    public Misleading(int mislead, int total) {
        this.mislead = mislead;
        this.total = total;
    }

    public Misleading sum(Misleading addend) {
        return new Misleading(this.mislead + addend.mislead, this.total + addend.total);
    }

    public double CriticalPercent() {
        return ((double) mislead / (double) total) * 100;
    }

}
