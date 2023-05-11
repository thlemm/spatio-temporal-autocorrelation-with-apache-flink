package de.thlemm.functions;


import de.thlemm.records.GlobalValue;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * A simple {@link ProcessAllWindowFunction}, which wraps {@link GlobalValue}s
 * and shifts the window time by a fixed span.
 *
 **/
public class GlobalAggregationCollectorWithIndexShift extends ProcessAllWindowFunction<Double, GlobalValue, TimeWindow> {

    private final TimeUnit STEP_SIZE;
    private final int TIME_WINDOW_LENGTH;

    public GlobalAggregationCollectorWithIndexShift(TimeUnit STEP_SIZE, int TIME_WINDOW_LENGTH) {
        this.STEP_SIZE = STEP_SIZE;
        this.TIME_WINDOW_LENGTH = TIME_WINDOW_LENGTH;
    }
    @Override
    public void process(
            final Context context,
            final Iterable<Double> elements,
            final Collector<GlobalValue> out) throws Exception {
        Double value = elements.iterator().next();
        out.collect(
                new GlobalValue(
                        new Date(context.window().getStart() + STEP_SIZE.toMillis(-TIME_WINDOW_LENGTH)),
                        new Date(context.window().getEnd() + STEP_SIZE.toMillis(-TIME_WINDOW_LENGTH)),
                        value));
    }
}