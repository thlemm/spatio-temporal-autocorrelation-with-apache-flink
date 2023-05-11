package de.thlemm;


import de.thlemm.constants.Locations;
import de.thlemm.constants.Parameters;
import de.thlemm.functions.*;
import de.thlemm.records.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;


import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;

// -pipeline.count 25
public class PermutationGlobalAndLocal {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputTopic = params.get("inputTopic", "output_mapper_14");

        String outputTopic1 = params.get("outputTopic", "output_moran_global_randomized");
        String outputTopic2 = params.get("outputTopic", "output_moran_local_randomized");

        Properties kafkaProps = createKafkaProperties(params);

        WatermarkStrategy<LocalValue> localWatermarkStrategy = WatermarkStrategy
                .<LocalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime());

        LocalValueDeserializationSchema localValueDeserializationSchema = new LocalValueDeserializationSchema();


        GlobalValueSerializationSchema globalIndexSerializationSchema = new GlobalValueSerializationSchema(outputTopic1);
        FlinkKafkaProducer<GlobalValue> kafkaSink1 = new FlinkKafkaProducer<>(
                outputTopic1,
                globalIndexSerializationSchema,
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        LocalValueSerializationSchema localIndexSerializationSchema = new LocalValueSerializationSchema(outputTopic2);
        FlinkKafkaProducer<LocalValue> kafkaSink2 = new FlinkKafkaProducer<>(
                outputTopic2,
                localIndexSerializationSchema,
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        DataStream<LocalValue> inputStream = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic, localValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(localWatermarkStrategy)
                .name("Kafka Source");

        // -pipeline.count 10
        int multiplication;
        try {
            multiplication = Integer.parseInt(params.get("pipeline.count"));
        }
        catch (NumberFormatException e)
        {
            multiplication = 1;
        }

        // DataStream<GlobalValue>[] globalPipelineList = new DataStream[multiplication-1];
        // DataStream<LocalValue>[] localPipelineList = new DataStream[multiplication-1];
        DataStream[][] pipelineList = new DataStream[multiplication-1][2];
        int i;
        for (i = 0; i < multiplication-1; i++) {
            pipelineList[i] = buildPipeline(inputStream, kafkaProps);
        }

        DataStream[] streams;
        streams = buildPipeline(inputStream, kafkaProps);

        DataStream<GlobalValue>[] globalPipelineList = new DataStream[multiplication-1];
        DataStream<GlobalValue>[] localPipelineList = new DataStream[multiplication-1];
        for (i = 0; i < multiplication-1; i++) {
            globalPipelineList[i] = pipelineList[i][0];
            localPipelineList[i] = pipelineList[i][1];
        }

        DataStream<GlobalValue> outputStream1 = streams[0].union(
                globalPipelineList
        );

        DataStream<LocalValue> outputStream2 = streams[1].union(
                localPipelineList
        );

        outputStream1
                .addSink(kafkaSink1)
        ;

        outputStream2
                .addSink(kafkaSink2)
        ;

        env.execute(multiplication + "x Randomization Pipeline");

    }

    private static DataStream[] buildPipeline(DataStream<LocalValue> inputStream, Properties kafkaProps) {
        final double n = Parameters.N;
        List<Integer> shuffledLocations = Locations.shuffleLocations(Locations.getLocations((int) n));


        // Shuffled locationIDs for every pipeline
        DataStream<LocalValue> localTimeSeriesDataStream = inputStream
                .process(new ProcessFunction<LocalValue, LocalValue>() {
                    @Override
                    public void processElement(LocalValue value, Context context, Collector<LocalValue> out) throws Exception {
                        int locationid = value.getLocationid();
                        value.setLocationid(shuffledLocations.get(locationid - 1));
                        // value.setWindowEnd(new Date(value.getWindowEnd().toInstant().toEpochMilli() - 20000));
                        // value.setWindowStart(new Date(value.getWindowStart().toInstant().toEpochMilli() - 20000));
                        out.collect(value);
                    }
                })
                ;

        // Aggregator 15
        DataStream<GlobalValue> aggregatedLocalTimeSeriesDataStream = localTimeSeriesDataStream
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueSquaredAggregator(),
                        // new GlobalAggregationCollector())
                        new GlobalAggregationCollectorWithSeriesShift(Parameters.STEP_SIZE, Parameters.TIME_SERIES_LENGTH))
                .name("Aggregator 15")
                ;

        // Mapper 16
        DataStream<LocalValue> localTimeSeriesCrossProductDataStream = localTimeSeriesDataStream
                .join(localTimeSeriesDataStream)
                .where(LocalValue::getWindowEnd)
                .equalTo(LocalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new WeightedLocalCrossProductMapper(Parameters.WEIGHTS_MATRIX))
                // Aggregator 17
                .keyBy(CrossValue::getLeftLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new CrossValueAggregator(),
                        // new LocalAggregationCollector())
                        new LocalAggregationCollectorWithSeriesShift(Parameters.STEP_SIZE, Parameters.TIME_SERIES_LENGTH))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LocalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                        .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime()))
                ;

        // Mapper 18
        DataStream<LocalValue> localTimeSeriesCrossProductDataStream2 =localTimeSeriesCrossProductDataStream
                .join(localTimeSeriesDataStream)
                .where(LocalValue::getLocationid)
                .equalTo(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalCrossProductMapper(Parameters.N))
                ;

        // Mapper 19
        DataStream<LocalValue> localIndexDataStream = localTimeSeriesCrossProductDataStream2
                .join(aggregatedLocalTimeSeriesDataStream)
                .where(LocalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalIndexMapper())
                ;

        // Mapper 20
        DataStream<GlobalValue> timeSeriesCrossProductDataStream = localTimeSeriesDataStream
                .join(localTimeSeriesDataStream)
                .where(LocalValue::getWindowEnd)
                .equalTo(LocalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new WeightedCrossProductMapper(Parameters.WEIGHTS_MATRIX))
                // Aggregator 21
                .keyBy(CrossValue::getLeftLocationid)
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new CrossValueAggregator(),
                        // new GlobalAggregationCollector())
                        new GlobalAggregationCollectorWithSeriesShift(Parameters.STEP_SIZE, Parameters.TIME_SERIES_LENGTH))
                .name("Aggregator 20")
                ;

        // Mapper 22
        DataStream<GlobalValue> globalIndexDataStream = timeSeriesCrossProductDataStream
                .join(aggregatedLocalTimeSeriesDataStream)
                .where(GlobalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new GlobalIndexMapper(Parameters.N, Parameters.W))
                .map(new MetricsMapper())
                ;

        if (Parameters.MEASURING_MODE) {
            // Emitting a LatencyValue
            DataStream<LatencyValue> latencyMeter = globalIndexDataStream
                    .map(new MapFunction<GlobalValue, LatencyValue>() {
                        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss:SSS")
                        @Override
                        public LatencyValue map(GlobalValue value) throws Exception {
                            Duration d =
                                    Duration.between(
                                            value.getWindowEnd().toInstant(),
                                            Instant.now()
                                    );

                            return new LatencyValue(
                                    value.getWindowStart(),
                                    value.getWindowEnd(),
                                    new Date(),
                                    d.toMillis()-Parameters.STEP_SIZE.toMillis(3*Parameters.TIME_WINDOW_LENGTH),
                                    value.getValue()
                            );
                        }
                    })
                    ;
            LatencyValueSerializationSchema latencyValueSerializationSchema = new LatencyValueSerializationSchema("output_latency_test");
            latencyMeter
                    .addSink(new FlinkKafkaProducer<>(
                            "output_latency_test",
                            latencyValueSerializationSchema,
                            kafkaProps,
                            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
                    );

        }

        DataStream[] output = new DataStream[2];
        output[0] = globalIndexDataStream;
        output[1] = localIndexDataStream;

        return output;
    }

    private static Properties createKafkaProperties(final ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "kafka:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }
}


