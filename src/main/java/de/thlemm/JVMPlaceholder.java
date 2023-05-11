package de.thlemm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import de.thlemm.constants.Parameters;
import de.thlemm.records.Event;
import de.thlemm.records.EventDeserializationSchema;

import java.util.Properties;

public class JVMPlaceholder {
        public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        final ParameterTool params = ParameterTool.fromArgs(args);

        String inputTopic = params.get("topic", "input_sample_data");

        Properties kafkaProps = createKafkaProperties(params);

        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Parameters.OUT_OF_ORDER_DURATION)
                .withTimestampAssigner((event, timestamp) -> event.getTpep_datetime().getTime());

        EventDeserializationSchema eventDeserializationSchema = new EventDeserializationSchema();

        DataStream<Event> inputStream = env.addSource(
                new FlinkKafkaConsumer<>(inputTopic, eventDeserializationSchema, kafkaProps))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("Kafka Source");

        env.execute("Placeholder for Taskmanager Test");
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
