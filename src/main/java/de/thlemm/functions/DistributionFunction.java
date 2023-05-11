package de.thlemm.functions;

import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import de.thlemm.records.GlobalList;
import de.thlemm.records.GlobalValue;

import java.util.Date;

public class DistributionFunction implements AllWindowFunction<GlobalValue, GlobalList, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<GlobalValue> g, Collector<GlobalList> out) throws Exception {
        double[] distList = new double[41];
        Date windowStart = IterableUtils.get(g, 0).getWindowStart();
        Date windowEnd = IterableUtils.get(g, 0).getWindowEnd();

        for (GlobalValue globalValue: g) {
            double value = globalValue.getValue();
            int index = (int) Math.ceil(value * 20) + 20; // round up to multiple of 0.05
            if (index >= 0 && index <= 40) {
                distList[index] = distList[index] + 1;
            }

        }
        out.collect(new GlobalList(windowStart, windowEnd, distList));
    }
}
