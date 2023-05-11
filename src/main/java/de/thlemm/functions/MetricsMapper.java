package de.thlemm.functions;

import de.thlemm.records.GlobalValue;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

public class MetricsMapper extends RichMapFunction<GlobalValue, GlobalValue> {
    private transient Counter counter;

    @Override
    public void open(Configuration config) {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
    }

    @Override
    public GlobalValue map(GlobalValue value) throws Exception {
        this.counter.inc();
        return value;
    }
}