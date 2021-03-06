package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateIncomeWritable implements Writable, Comparable {

    private String date;
    private int income;

    public DateIncomeWritable() {
    }

    public DateIncomeWritable(String date, int income) {
        this.date = date;
        this.income = income;
    }

    public DateIncomeWritable(DateIncomeWritable other) {
        this.date = other.date;
        this.income = other.income;
    }

    public String getDate() {
        return date;
    }

    public int getIncome() {
        return income;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
        out.writeInt(income);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date = in.readUTF();
        income = in.readInt();
    }

    @Override
    public int compareTo(Object o) {
        if (this.income != ((DateIncomeWritable) o).income)
            return Integer.compare(((DateIncomeWritable) o).income, this.income);
        else
            return this.date.compareTo(((DateIncomeWritable) o).date);
    }
}
