package com.hadoop.schema;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AvroProducer {
    private static final String TOPIC = "day-12-users";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp <jar> com.hadoop.schema.AvroProducer <configs-path>");
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

        System.out.println("=== Starting Java Schema-Aware Avro Producer ===");
        System.out.println("Bootstrap Servers: " + props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Schema Registry:   " + props.getProperty("schema.registry.url"));
        System.out.println("Target Topic:      " + TOPIC);

        KafkaProducer<String, User> producer = new KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 5; i++) {
                String userId = "usr_java_" + (100 + i);
                User user = User.newBuilder()
                        .setId(userId)
                        .setName("Java User " + i)
                        .setEmail("java.user" + i + "@example.com")
                        .setTimestamp(System.currentTimeMillis())
                        .build();

                System.out.println("[*] Producing record: " + user);

                ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC, userId, user);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("[-] Message delivery failed: " + exception.getMessage());
                        } else {
                            System.out.println("[✓] Message delivered to partition " + metadata.partition() 
                                    + " at offset " + metadata.offset());
                        }
                    }
                });
                
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            System.err.println("Producer interrupted: " + e.getMessage());
        } finally {
            System.out.println("[*] Flushing and closing Java producer...");
            producer.flush();
            producer.close();
            System.out.println("[✓] Java producer closed.");
        }
    }
}
