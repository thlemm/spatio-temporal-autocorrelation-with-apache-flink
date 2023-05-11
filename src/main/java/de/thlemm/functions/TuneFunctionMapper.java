package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.LocalValue;


public class TuneFunctionMapper implements JoinFunction<LocalValue, LocalValue, LocalValue> {

    @Override
    public LocalValue join(LocalValue cort, LocalValue v) throws Exception {

        final double value = (2 / (1 + Math.exp(2 * cort.getValue()))) * v.getValue();

        return new LocalValue(
                cort.getWindowStart(),
                cort.getWindowEnd(),
                cort.getLocationid(),
                value
        );
    }


}
