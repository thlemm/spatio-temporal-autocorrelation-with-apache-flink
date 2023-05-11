package de.thlemm.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import de.thlemm.records.LocalValue;

public class LocalAverageProcessor extends ProcessWindowFunction<LocalValue, LocalValue, Integer, TimeWindow> {

    private final double[][] weightsMatrix;

    public LocalAverageProcessor(double[][] weightsMatrix) {
        this.weightsMatrix = weightsMatrix;
    }

    @Override
    public void process(Integer key, Context context, Iterable<LocalValue> elements, Collector<LocalValue> out) {
        LocalValue element = elements.iterator().next();
        element.setValue(element.getValue() / (getRowSum(weightsMatrix[element.getLocationid()-1])));
        out.collect(element);

    }

    private int getRowSum(double[] rawList) {
        double w = 0;
        int lenght = rawList.length;
        for (int i = 0; i < lenght; i++) {
            if (rawList[i] != 0) {
                w += 1.0;
            }
        }
        return (int) Math.ceil(w);
    }
}
