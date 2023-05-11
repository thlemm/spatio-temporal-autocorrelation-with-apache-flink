package de.thlemm;

import de.thlemm.constants.Parameters;
import de.thlemm.functions.*;
import de.thlemm.records.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;


import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

public class Job {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputTopic = params.get("inputTopic", "input_sample_data");

        String outputTopicLocalTimeSeries = params.get("outputTopicLocalTimeSeries", "output_mapper_14");
        String outputTopicLocalTimeSeriesCrossProduct = params.get("outputTopicLocalTimeSeriesCrossProduct", "output_mapper_16");

        String outputTopicLocalIndex = params.get("outputTopicLocalIndex", "output_moran_local");
        String outputTopicGlobalIndex = params.get("outputTopicGlobalIndex", "output_moran_global");
        String outputTopicCheckSum = params.get("outputTopicCheckSum", "output_moran_checksum");


/*        String output_aggregator_1 = params.get("inputTopic", "output_aggregator_1");
        String output_aggregator_2 = params.get("inputTopic", "output_aggregator_2");
        String output_aggregator_3 = params.get("inputTopic", "output_aggregator_3");
        String output_aggregator_4 = params.get("inputTopic", "output_aggregator_4");
        String output_aggregator_5 = params.get("inputTopic", "output_aggregator_5");
        String output_aggregator_6 = params.get("inputTopic", "output_aggregator_6");
        String output_mapper_7 = params.get("inputTopic", "output_mapper_7");
        String output_mapper_8 = params.get("inputTopic", "output_mapper_8");

        String output_mapper_10 = params.get("inputTopic", "output_mapper_10");
        String output_aggregator_11 = params.get("inputTopic", "output_aggregator_11");
        String output_aggregator_12 = params.get("inputTopic", "output_aggregator_12");
        String output_mapper_13 = params.get("inputTopic", "output_mapper_13");
        String output_mapper_14 = params.get("inputTopic", "output_mapper_14");
        String output_aggregator_15 = params.get("inputTopic", "output_aggregator_15");

        String output_mapper_16 = params.get("inputTopic", "output_mapper_16");
        String output_mapper_18 = params.get("inputTopic", "output_mapper_18");
        String output_mapper_19 = params.get("inputTopic", "output_mapper_19");

        String output_mapper_20 = params.get("inputTopic", "output_mapper_20");
        String output_mapper_22 = params.get("inputTopic", "output_mapper_22");*/
        Properties kafkaProps = createKafkaProperties(params);

        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getTpep_datetime().getTime());

        WatermarkStrategy<GlobalValue> GlobalValueWatermarkStrategy = WatermarkStrategy
                .<GlobalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getWindowEnd().getTime());

        EventDeserializationSchema eventDeserializationSchema = new EventDeserializationSchema();

        DataStream<Event> inputStream = env.addSource(
                        new FlinkKafkaConsumer<>(inputTopic, eventDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("Kafka Source");

        if (Parameters.BACKPRESSURE_OPTION) {
            // Force a network shuffle so that the backpressure will affect the buffer pools
            inputStream = inputStream
                    .keyBy(Event::getLocationid)
                    .map(new BackpressureMap(1))
                    .name("Backpressure");
        }

        if (Parameters.MEASURING_MODE) {
            // Reassign event time by current server time
            inputStream = inputStream
                    .keyBy(Event::getLocationid)
                    .map(new TimeAssigner())
                    .assignTimestampsAndWatermarks(watermarkStrategy)
                    .name("TimeAssigner");

            DataStream<ThroughputValue> throughputMeter = inputStream
                    .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                    .aggregate(new EventAggregator(),
                            new ProcessAllWindowFunction<Double, ThroughputValue, TimeWindow>() {
                                @Override
                                public void process(Context context, Iterable<Double> elements, Collector<ThroughputValue> out) throws Exception {
                                    Double value = elements.iterator().next();
                                    out.collect(new ThroughputValue(
                                            new Date(context.window().getStart()),
                                            new Date(context.window().getEnd()),
                                            new Date(),
                                            value));
                                }
                            });
            ThroughputValueSerializationSchema throughputValueSerializationSchema = new ThroughputValueSerializationSchema("output_throughput_test");
            throughputMeter
                    .addSink(new FlinkKafkaProducer<>(
                            "output_throughput_test",
                            throughputValueSerializationSchema,
                            kafkaProps,
                            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
                    );

        }


        // Aggregator 1
        DataStream<LocalValue> localEventIncidenceDataStream = inputStream
                .keyBy(Event::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new EventAggregator(),
                        new EventAggregationCollector())
                .name("Aggregator 1");
/*        LocalValueSerializationSchema aggregator_01_schema = new LocalValueSerializationSchema(output_aggregator_1);
        localEventIncidenceDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_1,
                        aggregator_01_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 2
        DataStream<LocalValue> localTimeDeviationDataStream = localEventIncidenceDataStream
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.SUBTRACTOR_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .process(new LocalDeviationProcessor())
                .name("Aggregator 2");
/*        LocalValueSerializationSchema aggregator_02_schema = new LocalValueSerializationSchema(output_aggregator_2);
        localTimeDeviationDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_2,
                        aggregator_02_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 3
        DataStream<LocalValue> localTimeSeriesDeviationDataStream = localTimeDeviationDataStream
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueSquaredAggregator(),
                        // new LocalAggregationCollector())
                        new LocalAggregationCollectorWithIndexShift(Parameters.STEP_SIZE, Parameters.TIME_WINDOW_LENGTH))
                .name("Aggregator 3");
/*        LocalValueSerializationSchema aggregator_03_schema = new LocalValueSerializationSchema(output_aggregator_3);
        localTimeSeriesDeviationDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_3,
                        aggregator_03_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 4
        DataStream<GlobalValue> globalEventIncidenceDataStream = localEventIncidenceDataStream
                .keyBy(LocalValue::getLocationid)
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueAggregator(),
                        new GlobalAggregationCollector())
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .process(new ProcessAllWindowFunction<GlobalValue, GlobalValue, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<GlobalValue> elements, Collector<GlobalValue> out) {
                        GlobalValue element = elements.iterator().next();
                        element.setValue(element.getValue() / Parameters.N);
                        out.collect(element);
                    }
                })
                .name("Aggregator 4");
/*        GlobalValueSerializationSchema aggregator_04_schema = new GlobalValueSerializationSchema(output_aggregator_4);
        globalEventIncidenceDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_4,
                        aggregator_04_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 5
        DataStream<GlobalValue> globalTimeDeviationDataStream = globalEventIncidenceDataStream
                .windowAll(SlidingEventTimeWindows.of(Parameters.SUBTRACTOR_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .process(new GlobalDeviationProcessor())
                .name("Aggregator 5");
/*        GlobalValueSerializationSchema aggregator_05_schema = new GlobalValueSerializationSchema(output_aggregator_5);
        globalTimeDeviationDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_5,
                        aggregator_05_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 6
        DataStream<GlobalValue> globalTimeSeriesDeviationDataStream = globalTimeDeviationDataStream
                .windowAll(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new GlobalValueSquaredAggregator(),
                        // new GlobalAggregationCollector())
                        new GlobalAggregationCollectorWithIndexShift(Parameters.STEP_SIZE, Parameters.TIME_WINDOW_LENGTH))
                .name("Aggregator 6");
/*        GlobalValueSerializationSchema aggregator_06_schema = new GlobalValueSerializationSchema(output_aggregator_6);
        globalTimeSeriesDeviationDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_6,
                        aggregator_06_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 7
        DataStream<LocalValue> localTimeSeriesDeviationRootDataStream = localTimeSeriesDeviationDataStream
                .join(globalTimeSeriesDeviationDataStream)
                .where(LocalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalSquareRootMapper());
/*        LocalValueSerializationSchema aggregator_07_schema = new LocalValueSerializationSchema(output_mapper_7);
        localTimeSeriesDeviationRootDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_7,
                        aggregator_07_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 8
        DataStream<LocalValue> timeDeviationCrossProductDataStream = localTimeDeviationDataStream
                .join(globalTimeDeviationDataStream)
                .where(LocalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalMultipicationMapper())
                // Aggregator 9
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueAggregator(),
                        // new LocalAggregationCollector())
                        new LocalAggregationCollectorWithIndexShift(Parameters.STEP_SIZE, Parameters.TIME_WINDOW_LENGTH))
                .name("Aggregator 9");
/*        LocalValueSerializationSchema aggregator_08_schema = new LocalValueSerializationSchema(output_mapper_8);
        timeDeviationCrossProductDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_8,
                        aggregator_08_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 10
        DataStream<LocalValue> cortDataStream = localTimeSeriesDeviationRootDataStream
                .join(timeDeviationCrossProductDataStream)
                .where(LocalValue::getLocationid)
                .equalTo(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalDivisionMapper())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LocalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                        .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime()))
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalZeroValueFiller(Parameters.N));
/*        LocalValueSerializationSchema aggregator_10_schema = new LocalValueSerializationSchema(output_mapper_10);
        cortDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_10,
                        aggregator_10_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 11
        DataStream<LocalValue> localVolumeDataStream = localEventIncidenceDataStream
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueAggregator(),
                        new LocalAggregationCollector())
                .name("Aggregator 11");
/*        LocalValueSerializationSchema aggregator_11_schema = new LocalValueSerializationSchema(output_aggregator_11);
        localVolumeDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_11,
                        aggregator_11_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 12
        DataStream<GlobalValue> globalVolumeDataStream = globalEventIncidenceDataStream
                .windowAll(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new GlobalValueAggregator(),
                        new GlobalAggregationCollector())
                .name("Aggregator 12");
/*        GlobalValueSerializationSchema aggregator_12_schema = new GlobalValueSerializationSchema(output_aggregator_12);
        globalVolumeDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_12,
                        aggregator_12_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 13
        DataStream<LocalValue> localValueDifferenceDataStream = localVolumeDataStream
                .coGroup(globalVolumeDataStream)
                .where(LocalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalSubtractionCoGroupMapper(Parameters.N))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LocalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                        .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime()));
        /* LocalValueSerializationSchema mapper_13_schema = new LocalValueSerializationSchema(output_mapper_13);
        localValueDifferenceDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_13,
                        mapper_13_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 14
        DataStream<LocalValue> localTimeSeriesDataStream = cortDataStream
                .join(localValueDifferenceDataStream)
                .where(LocalValue::getLocationid)
                .equalTo(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new TuneFunctionMapper());
        /* LocalValueSerializationSchema mapper_14_schema = new LocalValueSerializationSchema(output_mapper_14);
        localTimeSeriesDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_14,
                        mapper_14_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 15
        DataStream<GlobalValue> aggregatedLocalTimeSeriesDataStream = localTimeSeriesDataStream
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueSquaredAggregator(),
                        // new GlobalAggregationCollector())
                        new GlobalAggregationCollectorWithSeriesShift(Parameters.STEP_SIZE, Parameters.TIME_SERIES_LENGTH))
                .name("Aggregator 15");
        /* GlobalValueSerializationSchema aggregator_15_schema = new GlobalValueSerializationSchema(output_aggregator_15);
        aggregatedLocalTimeSeriesDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_aggregator_15,
                        aggregator_15_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

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
                        .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime()));
        /* LocalValueSerializationSchema mapper_16_schema = new LocalValueSerializationSchema(output_mapper_16);
        localTimeSeriesCrossProductDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_16,
                        mapper_16_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 18
        DataStream<LocalValue> localTimeSeriesCrossProductDataStream2 = localTimeSeriesCrossProductDataStream
                .join(localTimeSeriesDataStream)
                .where(LocalValue::getLocationid)
                .equalTo(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalCrossProductMapper(Parameters.N));
        /* LocalValueSerializationSchema mapper_18_schema = new LocalValueSerializationSchema(output_mapper_18);
        localTimeSeriesCrossProductDataStream2
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_18,
                        mapper_18_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 19
        DataStream<LocalValue> localIndexDataStream = localTimeSeriesCrossProductDataStream2
                .join(aggregatedLocalTimeSeriesDataStream)
                .where(LocalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalIndexMapper());
/*        LocalValueSerializationSchema mapper_19_schema = new LocalValueSerializationSchema(output_mapper_19);
        localIndexDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_19,
                        mapper_19_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

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
                .name("Aggregator 20");
        /* GlobalValueSerializationSchema mapper_20_schema = new GlobalValueSerializationSchema(output_mapper_20);
        timeSeriesCrossProductDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_20,
                        mapper_20_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Mapper 22
        DataStream<GlobalValue> globalIndexDataStream = timeSeriesCrossProductDataStream
                .join(aggregatedLocalTimeSeriesDataStream)
                .where(GlobalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new GlobalIndexMapper(Parameters.N, Parameters.W))
                .map(new MetricsMapper());
        /* GlobalValueSerializationSchema mapper_22_schema = new GlobalValueSerializationSchema(output_mapper_22);
        globalIndexDataStream
                .addSink(new FlinkKafkaProducer<>(
                        output_mapper_22,
                        mapper_22_schema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;*/

        // Aggregator 22
        DataStream<GlobalValue> aggregatedLocalIndexDataStream = localIndexDataStream
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueAggregator(),
                        new GlobalAggregationCollector())
                .name("Aggregator 22");

        // Mapper 23
        DataStream<GlobalValue> checkSumDataStream = aggregatedLocalIndexDataStream
                .join(globalIndexDataStream)
                .where(GlobalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new CheckSumMapper(Parameters.N));

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
                                    d.toMillis() - Parameters.STEP_SIZE.toMillis(2 * Parameters.TIME_WINDOW_LENGTH),
                                    value.getValue()
                            );
                        }
                    });
            LatencyValueSerializationSchema latencyValueSerializationSchema = new LatencyValueSerializationSchema("output_latency_main_test");
            latencyMeter
                    .addSink(new FlinkKafkaProducer<>(
                            "output_latency_main_test",
                            latencyValueSerializationSchema,
                            kafkaProps,
                            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
                    );

        }


        LocalValueSerializationSchema localTimeSeriesSerializationSchema = new LocalValueSerializationSchema(outputTopicLocalTimeSeries);
        localTimeSeriesDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopicLocalTimeSeries,
                        localTimeSeriesSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;
        LocalValueSerializationSchema localTimeSeriesCrossProductSerializationSchema = new LocalValueSerializationSchema(outputTopicLocalTimeSeriesCrossProduct);
        localTimeSeriesCrossProductDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopicLocalTimeSeriesCrossProduct,
                        localTimeSeriesCrossProductSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;


        LocalValueSerializationSchema localIndexSerializationSchema = new LocalValueSerializationSchema(outputTopicLocalIndex);
        localIndexDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopicLocalIndex,
                        localIndexSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;

        GlobalValueSerializationSchema globalIndexSerializationSchema = new GlobalValueSerializationSchema(outputTopicGlobalIndex);
        globalIndexDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopicGlobalIndex,
                        globalIndexSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;

        GlobalValueSerializationSchema checkSumSerializationSchema = new GlobalValueSerializationSchema(outputTopicCheckSum);
        checkSumDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopicCheckSum,
                        checkSumSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
        ;
        System.out.println("output");
        System.out.println(env.getExecutionPlan());
        env.execute("Spatio-temporal autocorrelation in time series data");

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
