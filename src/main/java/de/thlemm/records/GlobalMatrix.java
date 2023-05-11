package de.thlemm.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

/**
 * A small wrapper class for list values without location.
 *
 */
public class GlobalMatrix {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowEnd;
    private double[][] matrix;

    public GlobalMatrix() {
    }

    public GlobalMatrix(
            final Date windowStart,
            final Date windowEnd,
            final double[][] matrix) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.matrix = matrix;
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

    public double[][] getMatrix() {
        return matrix;
    }

    public void setMatrix(final double[][] matrix) {
        this.matrix = matrix;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GlobalMatrix that = (GlobalMatrix) o;
        return matrix == that.matrix &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, matrix);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GlobalValue{");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd).append('\'');
        sb.append(", matrix=");
        sb.append(Arrays.toString(matrix));
        sb.append('}');
        return sb.toString();
    }
}
