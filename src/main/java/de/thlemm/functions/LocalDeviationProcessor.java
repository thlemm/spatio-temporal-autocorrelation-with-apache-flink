package de.thlemm.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import de.thlemm.records.LocalValue;

import org.apache.commons.collections4.IterableUtils;

import java.util.Date;

/**
 * A {@link ProcessWindowFunction}, which wraps {@link LocalValue}s
 * and subtracts the second element from the first element,
 * if there are more than one elements.
 *
 **/
public class LocalDeviationProcessor extends ProcessWindowFunction<LocalValue, LocalValue, Integer, TimeWindow> {

    @Override
    public void process(Integer key, Context context, Iterable<LocalValue> elements, Collector<LocalValue> out) {
        double deviation;
        int size = IterableUtils.size(elements);
        LocalValue firstElement = IterableUtils.get(elements, 0);
        if (size > 1) {
            LocalValue lastElement = IterableUtils.get(elements, IterableUtils.size(elements) - 1);
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
        out.collect(new LocalValue(new Date(context.window().getStart()), new Date(context.window().getEnd()), firstElement.getLocationid(), deviation));

    }
}
