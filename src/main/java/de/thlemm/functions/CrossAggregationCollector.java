package de.thlemm.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import de.thlemm.records.GlobalValue;

import java.util.Date;


/**
 * A simple {@link ProcessAllWindowFunction}, which wraps {@link GlobalValue}s
 *
 **/
public class CrossAggregationCollector extends ProcessAllWindowFunction<Double, GlobalValue, TimeWindow> {

    @Override
    public void process(
            final Context context,
            final Iterable<Double> elements,
            final Collector<GlobalValue> out) throws Exception {


        double value = elements.iterator().next();

        out.collect(new GlobalValue(new Date(context.window().getStart()), new Date(context.window().getEnd()), value));
    }
}