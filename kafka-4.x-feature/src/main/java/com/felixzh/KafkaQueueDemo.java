package com.felixzh;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaQueueDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "FelixZhnew:9092");
        props.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "FelixZhQueue");

        KafkaShareConsumer<String, String> kafkaShareConsumer =
                new KafkaShareConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        kafkaShareConsumer.subscribe(Collections.singletonList("test"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaShareConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
            kafkaShareConsumer.commitAsync();
        }
    }
}
