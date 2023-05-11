package de.thlemm.functions;

import de.thlemm.records.GlobalValue;
import de.thlemm.records.LocalValue;
import org.apache.flink.api.common.functions.JoinFunction;


public class LocalMultipicationMapper implements JoinFunction<LocalValue, GlobalValue, LocalValue> {

    @Override
    public LocalValue join(LocalValue localValue, GlobalValue globalValue) throws Exception {

        return new LocalValue(
                localValue.getWindowStart(),
                localValue.getWindowEnd(),
                localValue.getLocationid(),
                localValue.getValue() * globalValue.getValue());
    }
}
