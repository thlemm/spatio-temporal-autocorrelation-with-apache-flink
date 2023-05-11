package de.thlemm.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;
import java.util.Objects;

/**
 * A simple event writing a trip record on a {@link Event#locationid} at time {@link Event#tpep_datetime}
 *
 */
public class Event {

    //using java.util.Date for better readability
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
    private Date tpep_datetime;
    private int locationid;

    public Event() {
    }

    public Event(final Date tpep_datetime, int locationid) {
        this.tpep_datetime = tpep_datetime;
        this.locationid = locationid;
    }

    public Date getTpep_datetime() {
        return tpep_datetime;
    }

    public void setTpep_datetime(final Date tpep_datetime) {
        this.tpep_datetime = tpep_datetime;
    }


    public int getLocationid() {
        return locationid;
    }

    public void setLocationid(final int locationid) {
        this.locationid = locationid;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Event that = (Event) o;
        return Objects.equals(tpep_datetime, that.tpep_datetime) && locationid == that.locationid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tpep_datetime, locationid);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TripRecord{");
        sb.append("tpep_datetime=").append(tpep_datetime);
        sb.append(", locationid=").append(locationid);
        sb.append("}");
        return sb.toString();
    }
}
