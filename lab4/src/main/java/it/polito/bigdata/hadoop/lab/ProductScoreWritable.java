package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class ProductScoreWritable implements Writable {

    private String productID; // Contains the product ID
    private int score; // Contains the score

    public ProductScoreWritable(String productID, Integer score) {
        this.productID = productID;
        this.score = score;
    }

    public ProductScoreWritable(ProductScoreWritable other) {
        this.productID = other.productID;
        this.score = other.score;
    }

    public ProductScoreWritable() {
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public int getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(productID);
        dataOutput.writeInt(score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        productID = dataInput.readUTF();
        score = dataInput.readInt();
    }

    @Override
    public String toString() {
        return productID + "," + score;
    }
}
