package com.hadoop.schema;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumer {
    private static final String TOPIC = "day-12-users";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp <jar> com.hadoop.schema.AvroConsumer <configs-path>");
            System.exit(1);
        }

        String configPath = args[0];
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream(configPath)) {
            props.load(fis);
        } catch (IOException e) {
            System.err.println("Failed to load configuration from: " + configPath + ". Error: " + e.getMessage());
            System.exit(1);
        }

        System.out.println("=== Starting Java Schema-Aware Avro Consumer ===");
        System.out.println("Bootstrap Servers: " + props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Schema Registry:   " + props.getProperty("schema.registry.url"));
        System.out.println("Consumer Group:    " + props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        System.out.println("Subscribed Topic:  " + TOPIC);

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[*] Shutdown hook triggered. Waking up consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, User> record : records) {
                    System.out.println("[✓] Java Event Decoded Successfully!");
                    System.out.println("    - Key:       " + record.key());
                    System.out.println("    - Partition: " + record.partition());
                    System.out.println("    - Offset:    " + record.offset());
                    
                    User user = record.value();
                    System.out.println("    - Payload:   ID=" + user.getId() 
                            + ", Name=" + user.getName() 
                            + ", Email=" + user.getEmail() 
                            + ", Time=" + user.getTimestamp());
                }
                
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    System.out.println("[*] Offsets committed synchronously.");
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            System.out.println("[*] Consumer received wakeup signal. Exiting poll loop.");
        } catch (Exception e) {
            System.err.println("[X] Unexpected consumer error: " + e.getMessage());
        } finally {
            System.out.println("[*] Closing consumer connection...");
            consumer.close();
            System.out.println("[✓] Java consumer closed.");
        }
    }
}
