package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageWritable implements Writable {
    private double sum;
    private int nItems;

    public AverageWritable(){}

    public AverageWritable(double sum, int nItems) {
        this.sum = sum;
        this.nItems = nItems;
    }

    public AverageWritable sum(AverageWritable addend) {
        return new AverageWritable(this.sum + addend.sum, this.nItems + addend.nItems);
    }

    @Override
    public String toString(){
        return String.valueOf(sum / (double) nItems);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeInt(nItems);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        nItems = in.readInt();
    }
}
