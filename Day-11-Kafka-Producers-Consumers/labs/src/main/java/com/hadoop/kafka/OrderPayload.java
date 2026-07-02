package com.hadoop.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * Represents an E-Commerce Order Event payload serialized as JSON in Kafka.
 */
public class OrderPayload implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @JsonProperty("orderId")
    private String orderId;

    @JsonProperty("customerId")
    private String customerId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("status")
    private String status;

    @JsonProperty("timestamp")
    private long timestamp;

    // Default constructor for Jackson deserialization
    public OrderPayload() {}

    public OrderPayload(String orderId, String customerId, double amount, String status, long timestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Serializes this object into a JSON String.
     */
    public String toJsonString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize OrderPayload to JSON", e);
        }
    }

    /**
     * Deserializes a JSON String back into an OrderPayload object.
     */
    public static OrderPayload fromJsonString(String json) {
        try {
            return objectMapper.readValue(json, OrderPayload.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON to OrderPayload", e);
        }
    }

    @Override
    public String toString() {
        return "OrderPayload{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                ", status='" + status + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
