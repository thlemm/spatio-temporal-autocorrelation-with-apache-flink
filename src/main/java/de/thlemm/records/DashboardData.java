package de.thlemm.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

public class DashboardData {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowEnd;
    private double[] distData;
    private double meanDist;
    private double moranI;
    private double pvalue;
    private double[][] pvaluesLocal;
    private double[][] scatterData;
    private double regressionSlope;
    private double[][] moranData;

    public DashboardData() {
    }

    public DashboardData(
            final Date windowStart,
            final Date windowEnd,
            final double[] distData,
            final double meanDist,
            final double moranI,
            final double pvalue,
            final double[][] pvaluesLocal,
            final double[][] scatterData,
            final double regressionSlope,
            final double[][] moranData) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.distData = distData;
        this.meanDist = meanDist;
        this.moranI = moranI;
        this.pvalue = pvalue;
        this.pvaluesLocal = pvaluesLocal;
        this.scatterData = scatterData;
        this.regressionSlope = regressionSlope;
        this.moranData = moranData;
    }

    public Date getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(final Date windowStart) {
        this.windowStart = windowStart;
    }

    public Date getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(final Date windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double[] getDistData() {
        return distData;
    }

    public void setDistData(final double[] distData) {
        this.distData = distData;
    }

    public double getMeanDist() {
        return meanDist;
    }

    public void setMeanDist(double meanDist) {
        this.meanDist = meanDist;
    }

    public double getMoranI() {
        return moranI;
    }

    public void setMoranI(final double moranI) {
        this.moranI = moranI;
    }

    public double getPvalue() {
        return pvalue;
    }

    public void setPvalue(final double pvalue) {
        this.pvalue = pvalue;
    }

    public double[][] getPvaluesLocal() { return pvaluesLocal; }

    public void setPvaluesLocal(double[][] pvaluesLocal) { this.pvaluesLocal = pvaluesLocal; }

    public double[][] getScatterData() {
        return scatterData;
    }

    public void setScatterData(final double[][] scatterData) {
        this.scatterData = scatterData;
    }

    public double getRegressionSlope() {
        return regressionSlope;
    }

    public void setRegressionSlope(final double regressionSlope) {
        this.regressionSlope = regressionSlope;
    }

    public double[][] getMoranData() { return moranData; }

    public void setMoranData(double[][] moranData) { this.moranData = moranData; }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DashboardData that = (DashboardData) o;
        return distData == that.distData &&
                meanDist == that.meanDist &&
                moranI == that.moranI &&
                pvalue == that.pvalue &&
                pvaluesLocal == that.pvaluesLocal &&
                scatterData == that.scatterData &&
                regressionSlope == that.regressionSlope &&
                moranData == that.moranData &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, distData, meanDist, moranI, pvalue, pvaluesLocal, scatterData, regressionSlope, moranData);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GlobalValue{");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd);
        sb.append(", distData=");
        sb.append(Arrays.toString(distData));
        sb.append(", moranI=").append(moranI);
        sb.append(", meanDist=").append(meanDist);
        sb.append(", pvalue=").append(pvalue);
        sb.append(", pvaluesLocal=").append(Arrays.toString(pvaluesLocal));
        sb.append(", scatterData=");
        sb.append(Arrays.toString(scatterData));
        sb.append(", regressionSlope=").append(regressionSlope);
        sb.append(", moranData=");
        sb.append(Arrays.toString(moranData));
        sb.append('}');
        return sb.toString();
    }
}
