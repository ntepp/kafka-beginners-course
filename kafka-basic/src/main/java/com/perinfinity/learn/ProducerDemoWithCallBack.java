package com.perinfinity.learn;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        System.out.println("Producer Demo");
        // create the Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
        logger.info("Message sent: {}", "record");

        // Create a new Producer

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for(int j=0;j<10;j++){
                for(int i=0;i<30;i++){
                    // Create a producer record
                    ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello world");
                    // send data
                    producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception exception){
                            if(exception == null) {
                                logger.info("Received messsage data topic: {} \n- partition: {} \n- offset: {} \n- Timestamp: {}",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                            }
                        }
                    });
                    logger.info("Message sent: {}", record);
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            logger.error("Error sending message", e);
        }

        
        // flush and close the producer
    }
}