package de.thlemm.functions;

import de.thlemm.records.GlobalValue;
import org.apache.flink.api.common.functions.AggregateFunction;


/**
 * An {@link AggregateFunction} which subtracts {@link GlobalValue}s.
 *
 */
public class GlobalValueSubtractor implements AggregateFunction<GlobalValue, Double,
        Double> {
    @Override
    public Double createAccumulator() {
        return null;
    }

    @Override
    public Double add(final GlobalValue value, final Double accumulator) {
        if (accumulator == null) {
            return -1 * value.getValue();
        } else {
            return accumulator + value.getValue();
        }
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