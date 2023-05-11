package de.thlemm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import de.thlemm.records.CrossValue;
import de.thlemm.records.LocalValue;



public class WeightedCrossProductMapper implements JoinFunction<LocalValue, LocalValue, CrossValue> {

    private final double[][] weightsMatrix;

    public WeightedCrossProductMapper(double[][] weightsMatrix) {
        this.weightsMatrix = weightsMatrix;
    }

    @Override
    public CrossValue join(LocalValue leftValue, LocalValue rightValue) {

        final double weight = weightsMatrix[leftValue.getLocationid()-1][rightValue.getLocationid()-1];

        final double value = weight * leftValue.getValue() * rightValue.getValue();

        return new CrossValue(
                leftValue.getWindowStart(),
                leftValue.getWindowEnd(),
                leftValue.getLocationid(),
                rightValue.getLocationid(),
                value);
    }
}
