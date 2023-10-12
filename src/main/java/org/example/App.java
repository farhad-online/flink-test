package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.Serial;
import java.util.Properties;

public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello World!");
        String topicName = "gps_l";
        String kafkaServer = "";

        streamConsumer(topicName, kafkaServer);

    }

    public static void streamConsumer(String topic, String kafkaAddress) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(topic, kafkaAddress);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

        stringInputStream.map(new MapFunction<String, String>() {
            @Serial
            private static final long serialVersionUID = -999736771747691234L;

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();

        environment.execute();
    }

    public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", "group_ABCS");

        return new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
    }
}
