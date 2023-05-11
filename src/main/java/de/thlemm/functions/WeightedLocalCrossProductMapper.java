package de.thlemm.functions;

import de.thlemm.records.CrossValue;
import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.LocalValue;


public class WeightedLocalCrossProductMapper implements JoinFunction<LocalValue, LocalValue, CrossValue> {

    private final double[][] weightsMatrix;

    public WeightedLocalCrossProductMapper(double[][] weightsMatrix) {
        this.weightsMatrix = weightsMatrix;
    }

    @Override
    public CrossValue join(LocalValue leftValue, LocalValue rightValue) throws Exception {

        final double weight = weightsMatrix[leftValue.getLocationid()-1][rightValue.getLocationid()-1];

        final double value = weight * rightValue.getValue();

        return new CrossValue(
                leftValue.getWindowStart(),
                leftValue.getWindowEnd(),
                leftValue.getLocationid(),
                rightValue.getLocationid(),
                value);
    }
}
