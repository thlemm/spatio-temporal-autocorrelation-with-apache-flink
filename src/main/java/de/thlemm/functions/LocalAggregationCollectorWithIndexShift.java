package de.thlemm.functions;


import de.thlemm.records.LocalValue;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * A simple {@link ProcessWindowFunction}, which wraps {@link LocalValue}s
 *
 **/
public class LocalAggregationCollectorWithIndexShift extends ProcessWindowFunction<Double, LocalValue, Integer, TimeWindow> {
    private final TimeUnit STEP_SIZE;
    private final int TIME_WINDOW_LENGTH;

    public LocalAggregationCollectorWithIndexShift(TimeUnit STEP_SIZE, int TIME_WINDOW_LENGTH) {
        this.STEP_SIZE = STEP_SIZE;
        this.TIME_WINDOW_LENGTH = TIME_WINDOW_LENGTH;
    }
    @Override
    public void process(
            final Integer locationid,
            final Context context,
            final Iterable<Double> elements,
            final Collector<LocalValue> out) throws Exception {
        Double value = elements.iterator().next();
        out.collect(
                new LocalValue(
                        new Date(context.window().getStart() + STEP_SIZE.toMillis(-TIME_WINDOW_LENGTH)),
                        new Date(context.window().getEnd() + STEP_SIZE.toMillis(-TIME_WINDOW_LENGTH)),
                        locationid,
                        value));
    }
}