package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.GlobalValue;
import de.thlemm.records.LocalValue;


public class LocalIndexMapper implements JoinFunction<LocalValue, GlobalValue, LocalValue> {

    @Override
    public LocalValue join(LocalValue leftValue, GlobalValue rightValue) throws Exception {
        final double value = leftValue.getValue() / rightValue.getValue();

        return new LocalValue(
                leftValue.getWindowStart(),
                leftValue.getWindowEnd(),
                leftValue.getLocationid(),
                value
        );
    }


}
