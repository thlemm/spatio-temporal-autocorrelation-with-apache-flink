package de.thlemm;

import de.thlemm.constants.Parameters;
import de.thlemm.functions.BackpressureMap;
import de.thlemm.functions.GlobalValueAggregator;
import de.thlemm.records.*;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.Date;
import java.util.Properties;

public class DistributionAggregator {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool params = ParameterTool.fromArgs(args);


        String inputTopic3 = params.get("inputTopic3", "output_moran_global_randomized");
        String inputTopic4 = params.get("inputTopic4", "output_moran_global");

        String outputTopic = params.get("outputTopic", "output_dist_data");

        Properties kafkaProps = createKafkaProperties(params);

        // 60m_360m: Duration.ofHours(14) mit Backpressure 2ms in DatabaseSimulator
        WatermarkStrategy<GlobalValue> globalWatermarkStrategy = WatermarkStrategy
                .<GlobalValue>forBoundedOutOfOrderness(Duration.ofHours(4))
                .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime());

        GlobalValueDeserializationSchema globalValueDeserializationSchema = new GlobalValueDeserializationSchema();

        DataStream<GlobalValue> inputStream3 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic3, globalValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(globalWatermarkStrategy)
                .name("Kafka Source 3");

        DataStream<GlobalValue> inputStream4 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic4, globalValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(globalWatermarkStrategy)
                .name("Kafka Source 4");

        if (false) {
            // Force a network shuffle so that the backpressure will affect the buffer pools
            inputStream3 = inputStream3
                    .keyBy(GlobalValue::getWindowEnd)
                    .map(new MapFunction<GlobalValue, GlobalValue>() {
                        @Override
                        public GlobalValue map(GlobalValue value) throws Exception {
                            Thread.sleep(1);
                            return value;
                        }
                    })
                    .name("Backpressure");
            inputStream4 = inputStream4
                    .keyBy(GlobalValue::getWindowEnd)
                    .map(new MapFunction<GlobalValue, GlobalValue>() {
                        @Override
                        public GlobalValue map(GlobalValue value) throws Exception {
                            Thread.sleep(1);
                            return value;
                        }
                    })
                    .name("Backpressure");
        }
        System.out.println("test started");
        inputStream3 = inputStream3
                .keyBy(GlobalValue::getWindowEnd)
                .map(new MapFunction<GlobalValue, GlobalValue>() {
                    @Override
                    public GlobalValue map(GlobalValue value) throws Exception {
                        // System.out.println(value.toString());
                        return value;
                    }
                })
                .name("Backpressure");

        DataStream<GlobalValue> randomCountStream = inputStream3
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new AllWindowFunction<GlobalValue, GlobalValue, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<GlobalValue> values, Collector<GlobalValue> out) throws Exception {
                        Date windowStart = IterableUtils.get(values, 0).getWindowStart();
                        Date windowEnd = IterableUtils.get(values, 0).getWindowEnd();
                        System.out.println("window: " + window);
                        double n = IterableUtils.size(values);
                        for (GlobalValue globalValue: values) {
                            System.out.println(globalValue.toString());
                        }

                        out.collect(new GlobalValue(windowStart, windowEnd, n));
                    }
                })
                ;

        DataStream<GlobalValue> meanDistributionStream = inputStream3
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new AllWindowFunction<GlobalValue, GlobalValue, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<GlobalValue> values, Collector<GlobalValue> out) throws Exception {
                        Date windowStart = IterableUtils.get(values, 0).getWindowStart();
                        Date windowEnd = IterableUtils.get(values, 0).getWindowEnd();
                        double n = IterableUtils.size(values);

                        double r = 0.0;

                        for (GlobalValue globalValue: values) {
                            r += globalValue.getValue();
                        }
                        double m = r/n;

                        out.collect(new GlobalValue(windowStart, windowEnd, m));
                    }
                })
                ;

        DataStream<GlobalValue> pValueDataStream = inputStream3
                .join(inputStream4)
                .where(GlobalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<GlobalValue, GlobalValue, GlobalList>) (first, second) -> {
                    double[] tuple = new double[2];
                    tuple[0] = first.getValue();
                    tuple[1] = second.getValue();
                    return new GlobalList(first.getWindowStart(), first.getWindowEnd(), tuple);
                })
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new AllWindowFunction<GlobalList, GlobalValue, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<GlobalList> values, Collector<GlobalValue> out) throws Exception {
                        Date windowStart = IterableUtils.get(values, 0).getWindowStart();
                        Date windowEnd = IterableUtils.get(values, 0).getWindowEnd();
                        // double m = IterableUtils.size(values);

                        double r = 0.0;

                        for (GlobalList globalList: values) {
                            if (globalList.getList()[0] > globalList.getList()[1]){
                                r = r + 1.0;
                            }
                        }
                        double p = (r + 1) / ( 99 + 1);

                        out.collect(new GlobalValue(windowStart, windowEnd, p));
                    }
                })
                ;

        DataStream<GlobalList> distDataStream = meanDistributionStream
                .join(pValueDataStream)
                .where(GlobalValue::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<GlobalValue, GlobalValue, GlobalList>) (first, second) -> {
                    double[] tuple = new double[2];
                    tuple[0] = first.getValue();
                    tuple[1] = second.getValue();
                    return new GlobalList(first.getWindowStart(), first.getWindowEnd(), tuple);
                })
                .join(randomCountStream)
                .where(GlobalList::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<GlobalList, GlobalValue, GlobalList>) (first, second) -> {
                    double[] tuple = new double[3];
                    tuple[0] = first.getList()[0];
                    tuple[1] = first.getList()[1];
                    tuple[2] = second.getValue();;
                    return new GlobalList(first.getWindowStart(), first.getWindowEnd(), tuple);
                })
                ;


        GlobalListSerializationSchema distDataSerializationSchema = new GlobalListSerializationSchema(outputTopic);
        distDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopic,
                        distDataSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name(outputTopic)
                ;



        env.execute("Dist Data Aggregator");

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
