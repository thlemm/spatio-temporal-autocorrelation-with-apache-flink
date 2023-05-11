package de.thlemm;


import de.thlemm.constants.Parameters;
import de.thlemm.functions.*;
import de.thlemm.records.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
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


import java.util.Properties;

// -pipeline.count 10
public class PermutationGlobal {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputTopic = params.get("inputTopic", "input_sample_data");
        String outputTopic = params.get("outputTopic", "output_moran_global_randomized");

        Properties kafkaProps = createKafkaProperties(params);

        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getTpep_datetime().getTime());

        EventDeserializationSchema eventDeserializationSchema = new EventDeserializationSchema();


        GlobalValueSerializationSchema globalIndexSerializationSchema = new GlobalValueSerializationSchema(outputTopic);
        FlinkKafkaProducer<GlobalValue> kafkaSink = new FlinkKafkaProducer<>(
                outputTopic,
                globalIndexSerializationSchema,
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);



        DataStream<Event> inputStream = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic, eventDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(watermarkStrategy)
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

        DataStream<GlobalValue>[] pipelineList = new DataStream[multiplication-1];
        int i;
        for (i = 0; i < multiplication-1; i++) {
            pipelineList[i] = buildPipeline(inputStream);
        }

        DataStream<GlobalValue> outputStream = buildPipeline(inputStream).union(
                pipelineList
        );


        outputStream
                .addSink(kafkaSink)
        ;
        env.execute(multiplication + "x Randomization Pipeline");

    }

    private static DataStream<GlobalValue> buildPipeline(DataStream<Event> inputStream) {

        // Aggregator 1
        DataStream<LocalValue> localEventIncidenceDataStream = inputStream
                .keyBy(Event::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new EventAggregator(),
                        new ShuffledEventAggregationCollector(Parameters.N))
                .name("Shuffle Local Incidence");

        // Aggregator 2
        DataStream<LocalValue> localTimeDeviationDataStream = localEventIncidenceDataStream
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.SUBTRACTOR_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .process(new LocalDeviationProcessor())
                .name("Aggregator 2")
                ;

        // Aggregator 3
        DataStream<LocalValue> localTimeSeriesDeviationDataStream = localTimeDeviationDataStream
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueSquaredAggregator(),
                        new LocalAggregationCollectorWithIndexShift(Parameters.STEP_SIZE, Parameters.TIME_WINDOW_LENGTH))
                .name("Aggregator 3")
                ;

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
                        element.setValue(element.getValue()/ Parameters.N);
                        out.collect(element);
                    }
                })
                .name("Aggregator 4");

        // Aggregator 5
        DataStream<GlobalValue> globalTimeDeviationDataStream = globalEventIncidenceDataStream
                .windowAll(SlidingEventTimeWindows.of(Parameters.SUBTRACTOR_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .process(new GlobalDeviationProcessor())
                .name("Aggregator 5")
                ;

        // Aggregator 6
        DataStream<GlobalValue> globalTimeSeriesDeviationDataStream = globalTimeDeviationDataStream
                .windowAll(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new GlobalValueSquaredAggregator(),
                        new GlobalAggregationCollectorWithIndexShift(Parameters.STEP_SIZE, Parameters.TIME_WINDOW_LENGTH))
                .name("Aggregator 6")
                ;

        // Mapper 7
        DataStream<LocalValue> localTimeSeriesDeviationRootDataStream = localTimeSeriesDeviationDataStream
                .join(globalTimeSeriesDeviationDataStream)
                .where(LocalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalSquareRootMapper())
                ;

        // Mapper 8
        DataStream<LocalValue> timeDeviationCrossProductDataStream = localTimeDeviationDataStream
                .join(globalTimeDeviationDataStream)
                .where(LocalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalMultipicationMapper())
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueAggregator(),
                        new LocalAggregationCollectorWithIndexShift(Parameters.STEP_SIZE, Parameters.TIME_WINDOW_LENGTH))
                .name("Aggregator 9")
                ;

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

        // Aggregator 11
        DataStream<LocalValue> localVolumeDataStream = localEventIncidenceDataStream
                .keyBy(LocalValue::getLocationid)
                .window(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueAggregator(),
                        new LocalAggregationCollector())
                .name("Aggregator 11")
                ;

        // Aggregator 12
        DataStream<GlobalValue> globalVolumeDataStream = globalEventIncidenceDataStream
                .windowAll(SlidingEventTimeWindows.of(Parameters.TIME_SERIES_WINDOW_SIZE, Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new GlobalValueAggregator(),
                        new GlobalAggregationCollector())
                .name("Aggregator 12")
                ;

        // Mapper 13
        DataStream<LocalValue> localValueDifferenceDataStream = localVolumeDataStream
                .coGroup(globalVolumeDataStream)
                .where(LocalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new LocalSubtractionCoGroupMapper(Parameters.N))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LocalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                        .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime()))
                ;

        // Mapper 14
        DataStream<LocalValue> localTimeSeriesDataStream = cortDataStream
                .join(localValueDifferenceDataStream)
                .where(LocalValue::getLocationid)
                .equalTo(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new TuneFunctionMapper())
                ;

        // Aggregator 15
        DataStream<GlobalValue> aggregatedLocalTimeSeriesDataStream = localTimeSeriesDataStream
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new LocalValueSquaredAggregator(),
                        new GlobalAggregationCollector())
                .name("Aggregator 15")
                ;


        // Mapper 19
        DataStream<GlobalValue> timeSeriesCrossProductDataStream = localTimeSeriesDataStream
                .join(localTimeSeriesDataStream)
                .where(LocalValue::getWindowStart)
                .equalTo(LocalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new WeightedCrossProductMapper(Parameters.WEIGHTS_MATRIX))
                .keyBy(CrossValue::getLeftLocationid)
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new CrossValueAggregator(),
                        new CrossAggregationCollector())
                .name("Aggregator 20")
                ;

        // Mapper 21
        return timeSeriesCrossProductDataStream
                .join(aggregatedLocalTimeSeriesDataStream)
                .where(GlobalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new GlobalIndexMapper(Parameters.N, Parameters.W));
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

