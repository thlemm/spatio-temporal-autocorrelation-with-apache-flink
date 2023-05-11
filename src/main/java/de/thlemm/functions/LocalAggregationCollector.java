package de.thlemm.functions;


import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import de.thlemm.records.LocalValue;

import java.util.Date;


/**
 * A simple {@link ProcessWindowFunction}, which wraps {@link LocalValue}s
 *
 **/
public class LocalAggregationCollector extends ProcessWindowFunction<Double, LocalValue, Integer, TimeWindow> {

    @Override
    public void process(
            final Integer locationid,
            final Context context,
            final Iterable<Double> elements,
            final Collector<LocalValue> out) throws Exception {
        Double value = elements.iterator().next();
        out.collect(new LocalValue(new Date(context.window().getStart()), new Date(context.window().getEnd()), locationid, value));
    }
}