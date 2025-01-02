package com.perinfinity.learn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        logger.info("Producer Demo");
        // create the Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        logger.info("Message sent: {}", "record");

        // Create a new Producer

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("java_demo", "hello world");
            // send data
            producer.send(record);
            logger.info("Message sent: {}", record);
            //producer.flush();
        } catch (Exception e) {
            logger.error("Error sending message", e);
        }


        // flush and close the producer
    }
}