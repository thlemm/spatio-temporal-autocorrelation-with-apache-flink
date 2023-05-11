package de.thlemm.functions;

import de.thlemm.records.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 * This MapFunction causes backpressure per event by given value.
 */
public class BackpressureMap implements MapFunction<Event, Event>  {

    private int delay;
    public BackpressureMap (int delay) {
        this.delay=delay;
    }

    @Override
    public Event map(Event event) throws Exception {
        Thread.sleep(delay);
        return event;
    }
}
