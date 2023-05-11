package de.thlemm.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;
import java.util.Objects;

/**
 * A small wrapper class for local values with location.
 *
 */
public class CrossValue {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowEnd;
    private int leftLocationid;
    private int rightLocationid;
    private double value;

    public CrossValue() {
    }

    public CrossValue(
            final Date windowStart,
            final Date windowEnd,
            final int leftLocationid,
            final int rightLocationid,
            final double value) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.leftLocationid = leftLocationid;
        this.rightLocationid = rightLocationid;
        this.value = value;
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

    public int getLeftLocationid() {
        return leftLocationid;
    }

    public void setLeftLocationid(final int leftLocationid) {
        this.leftLocationid = leftLocationid;
    }

    public int getRightLocationid() {
        return rightLocationid;
    }

    public void setRightLocationid(final int rightLocationid) {
        this.rightLocationid = rightLocationid;
    }

    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CrossValue that = (CrossValue) o;
        return value == that.value &&
                leftLocationid == that.leftLocationid &&
                rightLocationid == that.rightLocationid &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, leftLocationid, rightLocationid, value);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndexedValue{");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd);
        sb.append(", leftLocationid='").append(leftLocationid);
        sb.append(", rightLocationid='").append(rightLocationid).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
