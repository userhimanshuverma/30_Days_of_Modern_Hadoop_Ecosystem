package com.hadoop.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

/**
 * Production-ready Manual Commit Kafka Consumer with Custom Rebalance Listener.
 */
public class OrderConsumer {
    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);
    private static final String DEFAULT_TOPIC = "orders";
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        logger.info("Initializing Kafka Order Consumer...");

        Properties props = new Properties();
        
        // Fallback default configurations
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processing-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // small batch to easily track manual offset commits

        // Load custom property file if specified in args
        if (args.length > 0 && args[0].endsWith(".properties")) {
            String configPath = args[0];
            try (InputStream input = Files.newInputStream(Paths.get(configPath))) {
                props.load(input);
                logger.info("Loaded custom consumer configuration from {}", configPath);
            } catch (Exception e) {
                logger.warn("Could not load property file {}. Falling back to defaults.", configPath, e);
            }
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final Thread mainThread = Thread.currentThread();

        // Register shutdown hook for graceful exit using WakeupException
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Waking up consumer thread...");
            consumer.wakeup();
            try {
                mainThread.join(); // wait for the main thread to complete shutdown
            } catch (InterruptedException e) {
                logger.error("Interrupted waiting for consumer thread to finish", e);
            }
        }));

        try {
            // Subscribe to the topic with a Rebalance Listener to manage transactions/offsets during cooperative rebalance
            consumer.subscribe(Collections.singletonList(DEFAULT_TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    logger.warn("REBALANCE TRIGGERED: Revoking partitions from this consumer: {}", partitions);
                    // Commit any pending offsets before partitions are re-assigned to avoid duplicate consumption
                    try {
                        logger.info("Committing offsets before partition revocation...");
                        consumer.commitSync();
                        logger.info("Offsets committed successfully for revoked partitions.");
                    } catch (CommitFailedException e) {
                        logger.error("Commit failed during partition revocation", e);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    logger.info("REBALANCE COMPLETE: Partitions assigned to this consumer: {}", partitions);
                }
            });

            logger.info("Subscribed to topic: {}. Beginning poll loop...", DEFAULT_TOPIC);

            while (true) {
                // Poll records with a 1-second timeout
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    continue;
                }

                logger.info("Fetched {} records in this poll batch.", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Deserialize JSON payload
                        OrderPayload order = OrderPayload.fromJsonString(record.value());
                        
                        logger.info("PROCESSED record - Key: {} | Partition: {} | Offset: {} | Payload: {}",
                                record.key(), record.partition(), record.offset(), order);

                        // Save current offset of the processed record + 1 (the next expected offset to read)
                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1, "Metadata: Processed order " + order.getOrderId())
                        );

                    } catch (Exception e) {
                        logger.error("Poison Pill encountered! Error processing record at Partition: {} Offset: {}",
                                record.partition(), record.offset(), e);
                        // In production, route poison pills to a Dead Letter Queue (DLQ) topic here
                    }
                }

                // Perform manual commit synchronously after processing the batch
                if (!currentOffsets.isEmpty()) {
                    try {
                        logger.info("Initiating synchronous manual commit of offsets: {}", currentOffsets);
                        consumer.commitSync(currentOffsets);
                        logger.info("Manual commit succeeded.");
                        currentOffsets.clear(); // Clear local offsets map for the next poll cycle
                    } catch (CommitFailedException e) {
                        logger.error("Synchronous offset commit failed! This can occur if the rebalance took longer than max.poll.interval.ms.", e);
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Received wakeup signal from shutdown hook. Closing consumer loop...");
        } catch (Exception e) {
            logger.error("Unexpected error in consumer loop", e);
        } finally {
            try {
                // Perform final sync commit of remaining offsets if any
                if (!currentOffsets.isEmpty()) {
                    logger.info("Committing final offsets before shutdown...");
                    consumer.commitSync(currentOffsets);
                }
            } catch (Exception e) {
                logger.error("Final commit failed during shutdown", e);
            } finally {
                consumer.close();
                logger.info("Consumer shutdown complete. Closed resources.");
            }
        }
    }
}
