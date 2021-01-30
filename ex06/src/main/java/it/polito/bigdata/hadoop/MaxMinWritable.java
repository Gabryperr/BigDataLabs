package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxMinWritable implements Writable {
    private double max;
    private double min;

    public MaxMinWritable() {
    }

    public MaxMinWritable(double max, double min) {
        this.max = max;
        this.min = min;
    }

    public MaxMinWritable aggregate(MaxMinWritable other) {
        return new MaxMinWritable(Math.max(this.max, other.max), Math.min(this.min, other.min));
    }

    @Override
    public String toString() {
        return "max=" + max + "_min=" + min;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(max);
        out.writeDouble(min);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        max = in.readDouble();
        min = in.readDouble();
    }
}
