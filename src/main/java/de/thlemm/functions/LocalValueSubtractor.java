package de.thlemm.functions;

import de.thlemm.records.LocalValue;
import org.apache.flink.api.common.functions.AggregateFunction;


/**
 * An {@link AggregateFunction} which subtracts {@link LocalValue}s.
 *
 */
public class LocalValueSubtractor implements AggregateFunction<LocalValue, Double,
        Double> {
    @Override
    public Double createAccumulator() {
        return null;
    }

    @Override
    public Double add(final LocalValue value, final Double accumulator) {
        if (accumulator != null) {
            return accumulator + value.getValue();
        } else {
            if (value.getValue() > 0.0) {
                return -1 * value.getValue();
            } else {
                return 0.0;
            }

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