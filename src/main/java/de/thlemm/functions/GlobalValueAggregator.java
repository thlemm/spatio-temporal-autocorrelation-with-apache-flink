package de.thlemm.functions;

import de.thlemm.records.GlobalValue;
import org.apache.flink.api.common.functions.AggregateFunction;


/**
 * An {@link AggregateFunction} which sums up {@link GlobalValue}s.
 *
 */
public class GlobalValueAggregator implements AggregateFunction<GlobalValue, Double,
        Double> {
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(final GlobalValue value, final Double accumulator) {

        return accumulator + value.getValue();
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