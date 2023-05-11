package de.thlemm.functions;

import de.thlemm.records.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class TimeAssigner implements MapFunction<Event, Event> {
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")

    @Override
    public Event map(Event event) throws Exception {
        event.setTpep_datetime(new Date());
        return event;
    }
}
