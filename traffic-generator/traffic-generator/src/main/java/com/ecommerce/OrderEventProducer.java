package com.ecommerce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class OrderEventProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        try {
            while (true) {

                String orderId = UUID.randomUUID().toString();
                String userId = "user-" + random.nextInt(1000);
                double amount = random.nextDouble() * 500;

                String event = String.format(
                        "{\"orderId\":\"%s\",\"userId\":\"%s\",\"amount\":%.2f}",
                        orderId, userId, amount
                );

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("orders", orderId, event);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent event to partition "
                                + metadata.partition()
                                + " with offset "
                                + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}