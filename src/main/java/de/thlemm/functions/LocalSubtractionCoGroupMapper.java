package de.thlemm.functions;

import de.thlemm.constants.Locations;
import de.thlemm.records.GlobalValue;
import de.thlemm.records.LocalValue;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class LocalSubtractionCoGroupMapper implements CoGroupFunction<LocalValue, GlobalValue, LocalValue> {

    private final double n;

    public LocalSubtractionCoGroupMapper(double n) {
        this.n = n;
    }


    @Override
    public void coGroup(Iterable<LocalValue> l, Iterable<GlobalValue> g, Collector<LocalValue> out) throws Exception {
        List<Integer> locations = new Locations().getLocations((int) n);
        GlobalValue globalValue = IterableUtils.get(g, 0);

        double volume;

        if (IterableUtils.size(g) > 0) {
            volume = globalValue.getValue();
        } else {
            volume = 0.0;
        }


        for (LocalValue localValue: l) {
            int locationid = localValue.getLocationid();
            double result = localValue.getValue() - volume;

            locations.remove(new Integer(locationid));
            out.collect(new LocalValue(
                    localValue.getWindowStart(),
                    localValue.getWindowEnd(),
                    localValue.getLocationid(),
                    result
            ));
        }

        for (int location: locations) {
            double result = 0 - volume;
            out.collect(new LocalValue(
                    globalValue.getWindowStart(),
                    globalValue.getWindowEnd(),
                    location,
                    result
            ));
        }

    }
}
