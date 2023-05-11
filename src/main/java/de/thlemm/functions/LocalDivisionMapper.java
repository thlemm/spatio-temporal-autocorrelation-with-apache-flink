package de.thlemm.functions;

import de.thlemm.records.LocalValue;
import org.apache.flink.api.common.functions.JoinFunction;


public class LocalDivisionMapper implements JoinFunction<LocalValue, LocalValue, LocalValue> {

    @Override
    public LocalValue join(LocalValue divider, LocalValue dominator) throws Exception {

        return new LocalValue(
                divider.getWindowStart(),
                divider.getWindowEnd(),
                divider.getLocationid(),
                dominator.getValue() / divider.getValue());
    }
}
