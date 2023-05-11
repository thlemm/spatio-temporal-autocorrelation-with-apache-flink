package de.thlemm.records;

        import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

        import java.util.Date;
        import java.util.Objects;

/**
 *
 *
 */
public class ThroughputValue {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowStart;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date windowEnd;
    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date timeRecieved;

    private double count;

    public ThroughputValue() {
    }

    public ThroughputValue(
            final Date windowStart,
            final Date windowEnd,
            final Date timeRecieved,
            final double count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.timeRecieved = timeRecieved;
        this.count = count;

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

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ThroughputValue that = (ThroughputValue) o;
        return count == that.count &&
                Objects.equals(timeRecieved, that.timeRecieved) &&
                Objects.equals(windowStart, that.windowStart) &&
                Objects.equals(windowEnd, that.windowEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, timeRecieved, count);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LocalValue{");
        sb.append("windowStart=").append(windowStart);
        sb.append(", windowEnd=").append(windowEnd);
        sb.append(", timeRecieved='").append(timeRecieved).append('\'');
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
