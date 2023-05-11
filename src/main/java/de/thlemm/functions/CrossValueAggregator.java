package de.thlemm.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import de.thlemm.records.CrossValue;


/**
 * An {@link AggregateFunction} which sums up {@link CrossValue}s.
 *
 */
public class CrossValueAggregator implements AggregateFunction<CrossValue, Double,
        Double> {
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(final CrossValue value, final Double accumulator) {

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