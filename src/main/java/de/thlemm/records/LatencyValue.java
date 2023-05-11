package de.thlemm.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Duration;
import java.util.Date;
import java.util.Objects;

/**
 * A small wrapper class for local values with location.
 *
 */
public class LatencyValue {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowEnd;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date timeRecieved;

    private long millis;

    private double checkSum;

    public LatencyValue() {
    }

    public LatencyValue(
            final Date windowStart,
            final Date windowEnd,
            final Date timeRecieved,
            final long millis,
            final double checkSum) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.timeRecieved = timeRecieved;
        this.millis = millis;
        this.checkSum = checkSum;

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

    public Date getTimeRecieved() {
        return timeRecieved;
    }

    public void setTimeRecieved(Date timeRecieved) {
        this.timeRecieved = timeRecieved;
    }

    public long getMillis() {
        return millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public double getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(double checkSum) {
        this.checkSum = checkSum;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LatencyValue that = (LatencyValue) o;
        return millis == that.millis &&
                checkSum == that.checkSum &&
                Objects.equals(timeRecieved, that.timeRecieved) &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, timeRecieved, millis, checkSum);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LocalValue{");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd);
        sb.append(", timeRecieved='").append(timeRecieved).append('\'');
        sb.append(", millis=").append(millis);
        sb.append(", checkSum=").append(checkSum);
        sb.append('}');
        return sb.toString();
    }
}
