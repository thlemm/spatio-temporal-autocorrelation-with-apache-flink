package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.GlobalValue;



public class GlobalIndexMapper implements JoinFunction<GlobalValue, GlobalValue, GlobalValue> {

    private final double n;
    private final double w;

    public GlobalIndexMapper(double n, double w) {
        this.n = n;
        this.w = w;
    }

    @Override
    public GlobalValue join(GlobalValue leftValue, GlobalValue rightValue) {

        final double value = (n/w) * (leftValue.getValue() / rightValue.getValue());

        return new GlobalValue(
                leftValue.getWindowStart(),
                leftValue.getWindowEnd(),
                value);
    }
}
