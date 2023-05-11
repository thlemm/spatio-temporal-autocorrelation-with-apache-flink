package de.thlemm.constants;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Parameters {
    /*
     Backpressure option:
     */
    public static final boolean BACKPRESSURE_OPTION = false;

    /*
     Backpressure option:
    */
    public static final boolean MEASURING_MODE = false;


    /*
     Primary time window parameters:
        These parameters can be modified to define the respected events.
     */
    public static final TimeUnit STEP_SIZE = TimeUnit.MINUTES;
    public static final int TIME_WINDOW_LENGTH = 5;
    public static final int TIME_SERIES_LENGTH = 360;
    public static final Duration OUT_OF_ORDER_DURATION = Duration.ofSeconds(30);

    // TIME_WINDOW_LENGTH = 60; Duration.ofHours(5);
    // TIME_WINDOW_LENGTH = 15; Duration.ofHours(2);
    // TIME_WINDOW_LENGTH = 5; Duration.ofHours(1);

    /*
     Secondary time window parameters:
        These parameters will be calculated from the primary time window parameters.
     */
    public static final Time COUNTER_WINDOW_SIZE = Time.of(TIME_WINDOW_LENGTH, STEP_SIZE);
    public static final Time SUBTRACTOR_WINDOW_SIZE = Time.of(2*TIME_WINDOW_LENGTH, STEP_SIZE);
    public static final Time TIME_SERIES_WINDOW_SIZE = Time.of(TIME_SERIES_LENGTH, STEP_SIZE);


    /*
     Primary neighbouring parameters:
        These parameters can be modified to set the used neighbouring method and matrix syntax.
        Current settings:
            Order: first
            Criterion: queen
            Weight: binary
            Standardization: row
     */
    public static final int[][] NEIGHBOURS = Weights.neighboursFirstOrder;
    public static final double[][] WEIGHTS_MATRIX = Weights.createBinaryWeightsMatrixRowStandardized(NEIGHBOURS);

    /*
     Secondary neighbouring parameters:
        These parameters will be calculated from the primary neighbouring parameters.
     */
    public static final double N = Weights.getN(NEIGHBOURS);
    public static final double W = Weights.getW(NEIGHBOURS);
}
