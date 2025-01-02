package com.perinfinity.learn;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGracefulShutdown {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemoGracefulShutdown.class);

    public static void main(String[] args) {
        logger.info("Consumer Demo");
        // create the Consumer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-first-application";
        String topic = "demo_java";

        // create the Consumer config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        

        Thread mainThread = Thread.currentThread();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // Let join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));

            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
            
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("Key: {}, Value: {}", record.key(), record.value());
                    logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                }
            }
            //producer.flush();
        } catch(WakeupException exception) {
            logger.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            logger.error("Unexpected Exception in the consumer", e);
        } finally {
            consumer.close();
            logger.info("The consumer is now gracefully shut down");
        }


        // flush and close the producer
    }
}