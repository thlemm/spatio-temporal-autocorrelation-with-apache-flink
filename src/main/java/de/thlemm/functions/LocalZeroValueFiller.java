package de.thlemm.functions;

import de.thlemm.constants.Locations;
import de.thlemm.records.LocalValue;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.List;

public class LocalZeroValueFiller implements AllWindowFunction<LocalValue, LocalValue, TimeWindow> {

    private final double n;

    public LocalZeroValueFiller(double n) {
        this.n = n;
    }

    @Override
    public void apply(TimeWindow window, Iterable<LocalValue> l, Collector<LocalValue> out) {
        List<Integer> locations = Locations.getLocations((int) n);

        Date windowStart = IterableUtils.get(l, 0).getWindowStart();
        Date windowEnd = IterableUtils.get(l, 0).getWindowEnd();

        for (LocalValue localValue: l) {
            int locationid = localValue.getLocationid();
            locations.remove(new Integer(locationid));

            out.collect(localValue);
        }

        for (int location: locations) {
            out.collect(new LocalValue(
                    windowStart,
                    windowEnd,
                    location,
                    0.0
            ));
        }
    }
}
