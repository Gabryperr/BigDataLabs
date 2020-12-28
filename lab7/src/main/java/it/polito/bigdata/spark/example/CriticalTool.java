package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class CriticalTool implements Serializable {

    private int linesCount;
    private int criticalCount;

    public CriticalTool(int linesCount, int criticalCount) {
        this.linesCount = linesCount;
        this.criticalCount = criticalCount;
    }

    public CriticalTool(CriticalTool other) {
        this.linesCount = other.linesCount;
        this.criticalCount = other.criticalCount;
    }

    public CriticalTool() {
    }

    public int getLinesCount() {
        return linesCount;
    }

    public void setLinesCount(int linesCount) {
        this.linesCount = linesCount;
    }

    public int getCriticalCount() {
        return criticalCount;
    }

    public void setCriticalCount(int criticalCount) {
        this.criticalCount = criticalCount;
    }

    public CriticalTool sum(CriticalTool addend) {
        return new CriticalTool(this.linesCount + addend.linesCount,
                this.criticalCount + addend.criticalCount);
    }

    public double getCriticality() {
        return (double) criticalCount / (double) linesCount;
    }
}
