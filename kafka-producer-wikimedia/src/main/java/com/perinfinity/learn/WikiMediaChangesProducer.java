package com.perinfinity.learn;


import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.StreamException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

public class WikiMediaChangesProducer {

    public static final Logger logger = LoggerFactory.getLogger(WikiMediaChangesProducer.class);

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";

        String wikimediaStreamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";


        EventSource.Builder eventSourceBuilder = new EventSource
                .Builder(URI.create(wikimediaStreamUrl));

        EventSource eventSource = eventSourceBuilder
                .build();

        try {
            eventSource.start();

            // Use the messages() method to get an Iterable of MessageEvents
            for (MessageEvent event : eventSource.messages()) {
                logger.info("Event received: " + event.getData());

                // Send the received event to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getData());
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Event sent to Kafka - Topic: {}, Partition: {}, Offset: {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Error sending event to Kafka", exception);
                    }
                });
            }

        } catch (StreamException e) {
            logger.error("Error in EventSource connection", e);
        } finally {
            logger.info("Closing connection to Wikimedia stream.");
            eventSource.close();
            logger.info("Connection to Wikimedia stream closed.");
        }


    }
}