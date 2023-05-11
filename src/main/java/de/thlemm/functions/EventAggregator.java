package de.thlemm.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import de.thlemm.records.Event;



/**
 * An {@link AggregateFunction} which simply counts {@link Event}s.
 *
 */
public class EventAggregator implements AggregateFunction<Event, Double,
        Double> {
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(final Event value, final Double accumulator) {

        return accumulator + 1;
    }

    @Override
    public Double getResult(final Double accumulator) {
        return accumulator;
    }

    @Override
    public Double merge(final Double a, final Double b) {
        return a + b;
    }

}