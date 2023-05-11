package de.thlemm.functions;

import de.thlemm.records.GlobalValue;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.commons.collections4.IterableUtils;

import java.util.Date;

/**
 * A {@link ProcessAllWindowFunction}, which wraps {@link GlobalValue}s
 * and subtracts the second element from the first element,
 * if there are more than one elements.
 *
 **/
public class GlobalDeviationProcessor extends ProcessAllWindowFunction<GlobalValue, GlobalValue, TimeWindow> {

    @Override
    public void process(Context context, Iterable<GlobalValue> elements, Collector<GlobalValue> out) {
        double deviation;
        int size = IterableUtils.size(elements);
        GlobalValue firstElement = IterableUtils.get(elements, 0);
        if (size > 1) {
            GlobalValue lastElement = IterableUtils.get(elements, IterableUtils.size(elements) - 1);
            deviation = lastElement.getValue() - firstElement.getValue();
        } else {
            long elementStart = firstElement.getWindowStart().getTime();
            long windowStart = context.window().getStart();
            if (elementStart > windowStart) {
                deviation = firstElement.getValue();
            } else {
                deviation = -1 * firstElement.getValue();
            }
        }
        out.collect(new GlobalValue(new Date(context.window().getStart()), new Date(context.window().getEnd()), deviation));

    }

}
