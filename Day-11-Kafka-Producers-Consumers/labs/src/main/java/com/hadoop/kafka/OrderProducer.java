package com.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Production-ready Idempotent Kafka Producer simulating transaction events.
 */
public class OrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);
    private static final String DEFAULT_TOPIC = "orders";
    private static final Random random = new Random();

    public static void main(String[] args) {
        logger.info("Initializing Kafka Order Producer...");

        // Load producer configurations
        Properties props = new Properties();
        
        // Default fallbacks in case config file is not loaded
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        // Idempotency & Reliability defaults
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        
        // Performance defaults
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024)); // 64KB

        // Try loading from external property file if supplied in args
        if (args.length > 0 && args[0].endsWith(".properties")) {
            String configPath = args[0];
            try (InputStream input = Files.newInputStream(Paths.get(configPath))) {
                props.load(input);
                logger.info("Loaded custom producer configuration from {}", configPath);
            } catch (Exception e) {
                logger.warn("Could not load property file {}. Falling back to defaults.", configPath, e);
            }
        }

        // Check if continuous mode is enabled
        boolean continuous = false;
        int messageCount = 100; // default number of events to produce
        for (String arg : args) {
            if (arg.equals("--continuous")) {
                continuous = true;
                break;
            }
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        logger.info("Kafka Producer successfully started. Client ID: {}", props.get(ProducerConfig.CLIENT_ID_CONFIG));

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Closing producer...");
            producer.close();
            logger.info("Producer successfully closed.");
        }));

        int count = 0;
        try {
            while (continuous || count < messageCount) {
                count++;
                
                String customerId = "cust_" + (random.nextInt(10) + 1); // 10 distinct customers to observe partitioning
                String orderId = UUID.randomUUID().toString();
                double amount = Math.round((10.0 + (990.0 * random.nextDouble())) * 100.0) / 100.0;
                String status = random.nextDouble() > 0.05 ? "CREATED" : "FAILED";
                long timestamp = System.currentTimeMillis();

                OrderPayload payload = new OrderPayload(orderId, customerId, amount, status, timestamp);
                String jsonMessage = payload.toJsonString();

                // Keying by customerId to ensure orders from the same customer always land in the same partition
                ProducerRecord<String, String> record = new ProducerRecord<>(DEFAULT_TOPIC, customerId, jsonMessage);

                logger.debug("Sending order {} for customer {}...", orderId, customerId);
                
                // Asynchronous Send with Callback
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("Failed to deliver message for order: {}", orderId, exception);
                        } else {
                            logger.info("Delivered payload. Key: {} -> Partition: {} | Offset: {} | Timestamp: {}", 
                                    customerId, metadata.partition(), metadata.offset(), metadata.timestamp());
                        }
                    }
                });

                // Control ingestion rate to make console outputs readable
                Thread.sleep(500); 
            }
        } catch (InterruptedException e) {
            logger.info("Ingestion loop interrupted.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error encountered in producer loop", e);
        } finally {
            if (!continuous) {
                logger.info("Completed producing {} messages. Shutting down...", count);
                producer.close();
            }
        }
    }
}
