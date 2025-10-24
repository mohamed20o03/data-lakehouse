package com.datalake.api.service;

import com.datalake.api.model.UploadJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Service responsible for sending jobs to RabbitMQ and querying basic queue stats.
 *
 * Notes:
 * - Queue bean declaration is provided in a separate configuration class (RabbitConfig).
 * - RabbitAdmin.getQueueProperties(...) returns a Map; we defensively handle numeric types.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitMQService {

    private final AmqpTemplate rabbitTemplate;
    private final RabbitAdmin rabbitAdmin;
    private final ObjectMapper objectMapper;

    @Value("${rabbitmq.queue.name}")
    private String queueName;

    /**
     * Send job to RabbitMQ queue as a JSON payload.
     * Converts the given UploadJob to JSON and publishes it to the configured queue.
     */
    public void sendJob(UploadJob job) {
        try {
            // Convert job to JSON
            String message = objectMapper.writeValueAsString(job);

            log.info("Sending job to RabbitMQ - JobId: {}, Queue: {}", job.getJobId(), queueName);

            // Send message to queue
            rabbitTemplate.convertAndSend(queueName, message);

            log.info("Job successfully sent to RabbitMQ: {}", job.getJobId());

        } catch (Exception e) {
            log.error("Failed to send job to RabbitMQ - JobId: {}", job.getJobId(), e);
            throw new RuntimeException("Failed to queue job: " + e.getMessage(), e);
        }
    }

    /**
     * Get queue statistics (message count, consumer count).
     * Returns a map with keys: queueName, messageCount, consumerCount, status, and optional error.
     */
    public Map<String, Object> getQueueStats() {
        Map<String, Object> stats = new HashMap<>();

        try {
            log.debug("Retrieving queue stats for: {}", queueName);

            // Get queue properties from RabbitMQ (returns java.util.Properties)
            Properties queueProperties = rabbitAdmin.getQueueProperties(queueName);

            if (queueProperties != null) {
                // Values might come back as Number or String; handle both
                Object messageCountObj = queueProperties.get("QUEUE_MESSAGE_COUNT");
                Object consumerCountObj = queueProperties.get("QUEUE_CONSUMER_COUNT");

                int messageCount = 0;
                int consumerCount = 0;

                if (messageCountObj instanceof Number) {
                    messageCount = ((Number) messageCountObj).intValue();
                } else if (messageCountObj != null) {
                    try { messageCount = Integer.parseInt(messageCountObj.toString()); } catch (NumberFormatException ignored) {}
                }

                if (consumerCountObj instanceof Number) {
                    consumerCount = ((Number) consumerCountObj).intValue();
                } else if (consumerCountObj != null) {
                    try { consumerCount = Integer.parseInt(consumerCountObj.toString()); } catch (NumberFormatException ignored) {}
                }

                stats.put("queueName", queueName);
                stats.put("messageCount", messageCount);
                stats.put("consumerCount", consumerCount);
                stats.put("status", "available");

                log.info("Queue stats - Messages: {}, Consumers: {}", messageCount, consumerCount);
            } else {
                log.warn("Queue properties are null for: {}", queueName);
                stats.put("queueName", queueName);
                stats.put("status", "unavailable");
                stats.put("error", "Queue not found or not accessible");
            }

        } catch (Exception e) {
            log.error("Error retrieving queue stats", e);
            stats.put("queueName", queueName);
            stats.put("status", "error");
            stats.put("error", e.getMessage());
        }

        return stats;
    }
}