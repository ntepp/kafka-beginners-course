package com.perinfinity.learn;

import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.beans.EventHandler;

public class WikiMediaChangesHandler extends WebSocketListener {

    private final Producer<String, String> kafkaProducer;
    private final String kafkaTopic;

    public WikimediaWebSocketListener(Producer<String, String> kafkaProducer, String kafkaTopic) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void onOpen(WebSocket webSocket, okhttp3.Response response) {
        System.out.println("Connected to Wikimedia stream...");
    }

    @Override
    public void onMessage(WebSocket webSocket, String text) {
        System.out.println("Event received: " + text);

        // Send the received event to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, text);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Event sent to Kafka - Topic: " + metadata.topic() +
                        ", Partition: " + metadata.partition() +
                        ", Offset: " + metadata.offset());
            } else {
                System.err.println("Error sending event to Kafka: " + exception.getMessage());
            }
        });
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, okhttp3.Response response) {
        System.err.println("Connection failed: " + t.getMessage());
    }

    @Override
    public void onClosing(WebSocket webSocket, int code, String reason) {
        System.out.println("Connection closing: " + reason);
        webSocket.close(code, reason);
    }

    @Override
    public void onClosed(WebSocket webSocket, int code, String reason) {
        System.out.println("Connection closed: " + reason);
    }
}
