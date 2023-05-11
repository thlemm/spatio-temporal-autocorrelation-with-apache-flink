package de.thlemm;

import de.thlemm.constants.Parameters;
import de.thlemm.functions.DistributionFunction;
import de.thlemm.functions.GlobalValueAggregator;
import de.thlemm.records.*;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;


import java.util.Date;
import java.util.Properties;

// -output.topic topic_name
public class CreateDashboardData {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool params = ParameterTool.fromArgs(args);

        // -output.topic topic_name

        String inputTopic1 = params.get("inputTopic1", "output_mapper_14");
        String inputTopic2 = params.get("inputTopic2", "output_mapper_16");
        String inputTopic3 = params.get("inputTopic3", "output_moran_global_randomized");
        String inputTopic4 = params.get("inputTopic4", "output_moran_global");
        String inputTopic5 = params.get("inputTopic5", "output_moran_local");
        String inputTopic6 = params.get("inputTopic6", "output_moran_local_randomized");

        String outputTopicDashboardData = params.get("output.topic", "dashboard_preprocessed_output");
        String outputTupleDataStream = params.get("outputTopic", "outputTupleDataStream");
        String outputSlopeDataStream = params.get("outputTopic", "outputSlopeDataStream");
        String outputScatterDataStream = params.get("outputTopic", "outputScatterDataStream");
        String outputMoranData = params.get("outputTopic", "outputMoranData");


        Properties kafkaProps = createKafkaProperties(params);

        WatermarkStrategy<LocalValue> localWatermarkStrategy = WatermarkStrategy
                .<LocalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime());

        LocalValueDeserializationSchema localValueDeserializationSchema = new LocalValueDeserializationSchema();

        DataStream<LocalValue> inputStream1 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic1, localValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(localWatermarkStrategy)
                .name("Kafka Source 1");

        DataStream<LocalValue> inputStream2 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic2, localValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(localWatermarkStrategy)
                .name("Kafka Source 2");

        DataStream<LocalValue> inputStream5 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic5, localValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(localWatermarkStrategy)
                .name("Kafka Source 5");

        DataStream<LocalValue> inputStream6 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic6, localValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(localWatermarkStrategy)
                .name("Kafka Source 6");

        WatermarkStrategy<GlobalValue> globalWatermarkStrategy = WatermarkStrategy
                .<GlobalValue>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getWindowStart().getTime());

        GlobalValueDeserializationSchema globalValueDeserializationSchema = new GlobalValueDeserializationSchema();

/*        DataStream<GlobalValue> inputStream3 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic3, globalValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(globalWatermarkStrategy)
                .name("Kafka Source 3");*/

        DataStream<GlobalValue> inputStream4 = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic4, globalValueDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(globalWatermarkStrategy)
                .name("Kafka Source 4");


        DataStream<LocalList> tupleDataStream = inputStream1
                .join(inputStream2)
                .where(LocalValue::getLocationid)
                .equalTo(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<LocalValue, LocalValue, LocalList>) (first, second) -> {
                    double[] values = new double[2];
                    values[0] = first.getValue();
                    values[1] = second.getValue();

                    return new LocalList(
                            first.getWindowStart(),
                            first.getWindowEnd(),
                            first.getLocationid(),
                            values);

                })
                ;
/*        LocalListSerializationSchema localListSerializationSchema = new LocalListSerializationSchema(outputTupleDataStream);
        tupleDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTupleDataStream,
                        localListSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name(outputTopicDashboardData)
        ;*/

        DataStream<GlobalValue> slopeDataStream = tupleDataStream
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new AggregateFunction<LocalList, double[], double[]>() {
                               @Override
                               public double[] createAccumulator() {
                                   return new double[2];
                               }

                               @Override
                               public double[] add(LocalList value, double[] accumulator) {
                                   accumulator[0] = accumulator[0] + value.getList()[0] * value.getList()[1];
                                   accumulator[1] = accumulator[1] + value.getList()[0] * value.getList()[0];
                                   return accumulator;
                               }

                               @Override
                               public double[] getResult(double[] accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public double[] merge(double[] a, double[] b) {
                                   double[] merged = new double[2];
                                   merged[0] = a[0] + b[0];
                                   merged[1] = a[1] + b[1];
                                   return merged;
                               }
                           },
                        new ProcessAllWindowFunction<double[], GlobalList, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<double[]> elements, Collector<GlobalList> out) {
                                double[] list = elements.iterator().next();
                                out.collect(new GlobalList(
                                        new Date(context.window().getStart()),
                                        new Date(context.window().getStart() + Parameters.STEP_SIZE.toMillis(Parameters.TIME_SERIES_LENGTH)),
                                        list));
                            }
                        })
                .map((MapFunction<GlobalList, GlobalValue>) value -> {
                    double result = value.getList()[0] / value.getList()[1];
                    return new GlobalValue(value.getWindowStart(), value.getWindowEnd(), result);
                }).name("slope")
                ;
/*        GlobalValueSerializationSchema globalValueSerializationSchema = new GlobalValueSerializationSchema(outputSlopeDataStream);
        slopeDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputSlopeDataStream,
                        globalValueSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name(outputTopicDashboardData)
        ;*/


        DataStream<GlobalMatrix> scatterDataStream = tupleDataStream
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .process(new ProcessAllWindowFunction<LocalList, GlobalMatrix, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<LocalList> elements, Collector<GlobalMatrix> out) {
                        Date windowStart = IterableUtils.get(elements, 0).getWindowStart();
                        Date windowEnd = IterableUtils.get(elements, 0).getWindowEnd();

                        int size = IterableUtils.size(elements);
                        double[][] matrix = new double[size][2];
                        // double[][] matrix = new double[263][2];
                        int i;
                        for (i = 0; i < size; i++) {
                            int index = IterableUtils.get(elements, i).getLocationid()-1;
                            matrix[index] = IterableUtils.get(elements, i).getList();
                        }
                        System.out.println(matrix);
                        out.collect(new GlobalMatrix(windowStart, windowEnd, matrix));

                    }
                }).name("scatterData")
                ;
                GlobalMatrixSerializationSchema globalMatrixSerializationSchema = new GlobalMatrixSerializationSchema(outputScatterDataStream);
                scatterDataStream
                        .addSink(new FlinkKafkaProducer<>(
                                outputScatterDataStream,
                                globalMatrixSerializationSchema,
                                kafkaProps,
                                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                        .name(outputTopicDashboardData)
                ;

/*        DataStream<GlobalList> distributionStream = inputStream3
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new DistributionFunction())
                ;*/

/*        DataStream<GlobalValue> meanDistributionStream = inputStream3
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new GlobalValueAggregator(),
                        new AllWindowFunction<Double, GlobalValue, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Double> values, Collector<GlobalValue> out) throws Exception {
                                int size = IterableUtils.size(values);
                                double value = values.iterator().next()/size;
                                out.collect(new GlobalValue(new Date(window.getStart()), new Date(window.getEnd()), value));
                            }
                        })
                ;*/

       /* DataStream<GlobalValue> pValueDataStream = inputStream3
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
                ;*/

           DataStream<GlobalMatrix> pValuesLocalDataStream = inputStream6
                .keyBy(LocalValue::getLocationid)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new WindowFunction<LocalValue, LocalList, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer locationID, TimeWindow window, Iterable<LocalValue> input, Collector<LocalList> out) throws Exception {
                        Date windowStart = IterableUtils.get(input, 0).getWindowStart();
                        Date windowEnd = IterableUtils.get(input, 0).getWindowEnd();

                        int size = IterableUtils.size(input);
                        double[] list = new double[size];
                        int i;
                        for (i = 0; i < size; i++) {
                            list[i] = IterableUtils.get(input, i).getValue();
                        }
                        out.collect(new LocalList(windowStart, windowEnd, locationID, list));
                    }
                })
                .join(inputStream5)
                .where(LocalList::getWindowStart)
                .equalTo(LocalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new JoinFunction<LocalList, LocalValue, LocalValue>() {
                    @Override
                    public LocalValue join(LocalList first, LocalValue second) throws Exception {
                        double r = 0.0;

                        for (double value: first.getList()) {
                            if (value > second.getValue()){
                                r = r + 1.0;
                            }
                        }
                        double p = (r + 1) / ( 99 + 1);
                        // System.out.println(p);
                        return new LocalValue(second.getWindowStart(), second.getWindowEnd(), second.getLocationid(), p);
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new AggregateFunction<LocalValue, double[][], double[][]>() {
                       @Override
                       public double[][] createAccumulator() {
                           return new double[(int) Parameters.N][];
                       }

                       @Override
                       public double[][] add(LocalValue value, double[][] accumulator) {
                           double[] tuple = new double[2];
                           tuple[0] = value.getLocationid();
                           tuple[1] = value.getValue();
                           accumulator[value.getLocationid()-1] = tuple;
                           return accumulator;
                       }

                       @Override
                       public double[][] getResult(double[][] accumulator) {
                           return accumulator;
                       }

                       @Override
                       public double[][] merge(double[][] a, double[][] b) {
                           return new double[0][];
                       }
                   },
                new ProcessAllWindowFunction<double[][], GlobalMatrix, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<double[][]> elements, Collector<GlobalMatrix> out) {
                        double[][] list = elements.iterator().next();
                        out.collect(new GlobalMatrix(new Date(context.window().getStart()), new Date(context.window().getEnd()), list));
                    }
                })
                ;



        DataStream<GlobalMatrix> moranData = inputStream5
                .windowAll(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .aggregate(new AggregateFunction<LocalValue, double[][], double[][]>() {
                    @Override
                    public double[][] createAccumulator() {
                        return new double[(int) Parameters.N][];
                    }

                    @Override
                    public double[][] add(LocalValue value, double[][] accumulator) {
                        double[] tuple = new double[2];
                        tuple[0] = value.getLocationid();
                        tuple[1] = value.getValue();
                        accumulator[value.getLocationid()-1] = tuple;
                        return accumulator;
                    }

                    @Override
                    public double[][] getResult(double[][] accumulator) {
                        return accumulator;
                    }

                    @Override
                    public double[][] merge(double[][] a, double[][] b) {
                        return new double[0][];
                    }
                },
                new ProcessAllWindowFunction<double[][], GlobalMatrix, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<double[][]> elements, Collector<GlobalMatrix> out) {
                        double[][] list = elements.iterator().next();
                        out.collect(new GlobalMatrix(
                                new Date(context.window().getStart()),
                                new Date(context.window().getStart() + Parameters.STEP_SIZE.toMillis(Parameters.TIME_SERIES_LENGTH)),
                                list));
                    }
                }).name("localMoranData")
                ;
/*        GlobalMatrixSerializationSchema globalMatrixSerializationSchema2 = new GlobalMatrixSerializationSchema(outputMoranData);
        moranData
                .addSink(new FlinkKafkaProducer<>(
                        outputMoranData,
                        globalMatrixSerializationSchema2,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name(outputTopicDashboardData)
        ;*/

/*
        DataStream<DashboardData> dashboardDataDataStream = distributionStream
                .join(matrixDataStream)
                .where(GlobalList::getWindowStart)
                .equalTo(GlobalMatrix::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<GlobalList, GlobalMatrix, DashboardData>) (first, second) ->
                        new DashboardData(first.getWindowStart(), first.getWindowEnd(), first.getList(), 0.0, 0.0, 0.0, null, second.getMatrix(),0.0, null))

                .join(slopeDataStream)
                .where(DashboardData::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<DashboardData, GlobalValue, DashboardData>) (first, second) -> {
                    first.setRegressionSlope(second.getValue());
                    return first;
                })*/
        DataStream<DashboardData> dashboardDataDataStream = slopeDataStream
                .join(inputStream4)
                .where(GlobalValue::getWindowEnd)
                .equalTo(GlobalValue::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<GlobalValue, GlobalValue, DashboardData>) (first, second) -> new DashboardData(
                        first.getWindowStart(), first.getWindowEnd(),
                        null, 0.0, second.getValue(), 0.0,
                        null, null, first.getValue(), null))
/*               .join(pValueDataStream)
                .where(DashboardData::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<DashboardData, GlobalValue, DashboardData>) (first, second) -> {
                    first.setPvalue(second.getValue());
                    return first;
                })*/
               .join(pValuesLocalDataStream)
                .where(DashboardData::getWindowStart)
                .equalTo(GlobalMatrix::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<DashboardData, GlobalMatrix, DashboardData>) (first, second) -> {
                    first.setPvaluesLocal(second.getMatrix());
                    return first;
                })
                .join(moranData)
                .where(DashboardData::getWindowEnd)
                .equalTo(GlobalMatrix::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<DashboardData, GlobalMatrix, DashboardData>) (first, second) -> {
                    first.setMoranData(second.getMatrix());
                    return first;
                })/*
                .join(scatterDataStream)
                .where(DashboardData::getWindowEnd)
                .equalTo(GlobalMatrix::getWindowEnd)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply((JoinFunction<DashboardData, GlobalMatrix, DashboardData>) (first, second) -> {
                    first.setScatterData(second.getMatrix());
                    return first;
                })*/
/*                .join(meanDistributionStream)
                .where(DashboardData::getWindowStart)
                .equalTo(GlobalValue::getWindowStart)
                .window(TumblingEventTimeWindows.of(Parameters.COUNTER_WINDOW_SIZE))
                .apply(new JoinFunction<DashboardData, GlobalValue, DashboardData>() {
                    @Override
                    public DashboardData join(DashboardData first, GlobalValue second) {
                        first.setMeanDist(second.getValue());
                        return first;
                    }
                })*/
                ;



        DashboardDataSerializationSchema dashboardDataSerializationSchema = new DashboardDataSerializationSchema(outputTopicDashboardData);
        dashboardDataDataStream
                .addSink(new FlinkKafkaProducer<>(
                        outputTopicDashboardData,
                        dashboardDataSerializationSchema,
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name(outputTopicDashboardData)
        ;


        env.execute("Dashboard Data Collector");

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
