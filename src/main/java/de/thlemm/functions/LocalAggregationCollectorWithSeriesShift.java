package de.thlemm.functions;

import de.thlemm.records.GlobalValue;
import de.thlemm.records.LocalValue;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * A simple {@link ProcessAllWindowFunction}, which wraps {@link LocalValue}s
 * and shifts the window start by a fixed span.
 *
 **/
public class LocalAggregationCollectorWithSeriesShift extends ProcessWindowFunction<Double, LocalValue, Integer, TimeWindow> {

    private final TimeUnit STEP_SIZE;
    private final int TIME_SERIES_LENGTH;

    public LocalAggregationCollectorWithSeriesShift(TimeUnit STEP_SIZE, int TIME_SERIES_LENGTH) {
        this.STEP_SIZE = STEP_SIZE;
        this.TIME_SERIES_LENGTH = TIME_SERIES_LENGTH;
    }
    @Override
    public void process(
            final Integer left_locationid,
            final Context context,
            final Iterable<Double> elements,
            final Collector<LocalValue> out) throws Exception {


        double value = elements.iterator().next();

        out.collect(new LocalValue(
                new Date(context.window().getStart()),
                new Date(context.window().getStart() + STEP_SIZE.toMillis(TIME_SERIES_LENGTH)),
                left_locationid,
                value));
    }
}