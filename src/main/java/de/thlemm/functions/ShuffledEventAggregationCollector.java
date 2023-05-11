package de.thlemm.functions;

import de.thlemm.records.LocalValue;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import de.thlemm.constants.Locations;

import java.util.Date;
import java.util.List;

/**
 * A simple {@link ProcessWindowFunction}, which wraps {@link LocalValue}
 *
 **/

public class ShuffledEventAggregationCollector extends ProcessWindowFunction<Double, LocalValue, Integer, TimeWindow> {

    private final double n;
    private List<Integer> locations;

    public ShuffledEventAggregationCollector(double n) {

        this.n = n;
        this.locations = Locations.shuffleLocations(Locations.getLocations((int) n));
    }



    @Override
    public void process(
            final Integer locationid,
            final Context context,
            final Iterable<Double> elements,
            final Collector<LocalValue> out) throws Exception {
        Double value = elements.iterator().next();
        out.collect(new LocalValue(new Date(context.window().getStart()), new Date(context.window().getEnd()), locations.get(locationid - 1), value));
    }
}