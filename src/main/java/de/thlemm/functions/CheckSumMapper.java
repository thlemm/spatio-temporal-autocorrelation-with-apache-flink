package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.GlobalValue;


public class CheckSumMapper implements JoinFunction<GlobalValue, GlobalValue, GlobalValue> {

    private final double n;

    public CheckSumMapper(double n) {
        this.n = n;
    }

    @Override
    public GlobalValue join(GlobalValue leftValue, GlobalValue rightValue) {

        final double value = (leftValue.getValue() / n) - rightValue.getValue();

        return new GlobalValue(
                rightValue.getWindowStart(),
                rightValue.getWindowEnd(),
                value);
    }
}
