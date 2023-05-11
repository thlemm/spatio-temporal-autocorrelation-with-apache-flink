package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.GlobalValue;
import de.thlemm.records.LocalValue;


public class LocalSubtractionMapper implements JoinFunction<LocalValue, GlobalValue, LocalValue> {

    @Override
    public LocalValue join(LocalValue localValue, GlobalValue globalValue) {

        return new LocalValue(
                localValue.getWindowStart(),
                localValue.getWindowEnd(),
                localValue.getLocationid(),
                localValue.getValue() - globalValue.getValue()
        );
    }
}
