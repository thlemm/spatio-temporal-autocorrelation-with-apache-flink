package de.thlemm.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;
import java.util.Objects;

/**
 * A small wrapper class for local values with location.
 *
 */
public class LocalValue {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowEnd;
    private int locationid;
    private double value;

    public LocalValue() {
    }

    public LocalValue(
            final Date windowStart,
            final Date windowEnd,
            final int locationid,
            final double value) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.locationid = locationid;
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

    public int getLocationid() {
        return locationid;
    }

    public void setLocationid(final int locationid) {
        this.locationid = locationid;
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
        final LocalValue that = (LocalValue) o;
        return value == that.value &&
                locationid == that.locationid &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, locationid, value);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LocalValue{");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd);
        sb.append(", locationid='").append(locationid).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
