package com.perinfinity.learn;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        System.out.println("Producer Demo");
        // create the Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        logger.info("Message sent: {}", "record");

        // Create a new Producer

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for(int i=0;i<2;i++){
            for(int j=0;j<10;j++){
                String topic = "demo_java"; 
                String key="truck_id_"+j;
                String value= "hello world "+j;
                // Create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                // send data
                producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception exception){
                            if(exception == null) {
                                logger.info("Key: {} | partition: {}",
                                key,  metadata.partition());
                            }
                        }
                    });
                logger.info("Message sent: {}", record);
                //producer.flush();
            }}
        } catch (Exception e) {
            logger.error("Error sending message", e);
        }


        // flush and close the producer
    }
}