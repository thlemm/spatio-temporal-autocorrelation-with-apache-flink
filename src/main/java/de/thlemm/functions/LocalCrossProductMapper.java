package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.LocalValue;


public class LocalCrossProductMapper implements JoinFunction<LocalValue, LocalValue, LocalValue> {

    private final double n;

    public LocalCrossProductMapper(double n) {
        this.n = n;
    }

    @Override
    public LocalValue join(LocalValue leftValue, LocalValue rightValue) throws Exception {

        final double value = n * rightValue.getValue() * leftValue.getValue();
        return new LocalValue(
                leftValue.getWindowStart(),
                leftValue.getWindowEnd(),
                leftValue.getLocationid(),
                value
        );
    }
}
