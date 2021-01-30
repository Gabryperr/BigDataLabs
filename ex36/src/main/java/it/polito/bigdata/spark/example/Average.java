package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class Average implements Serializable {
    double sum;
    int count;

    public Average() {
    }

    public Average(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public Average Add(Average addend) {
        return new Average(this.sum + addend.sum, this.count + addend.count);
    }

    @Override
    public String toString() {
        return "" + sum / (double) count;
    }
}
