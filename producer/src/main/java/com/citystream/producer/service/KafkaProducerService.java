package com.citystream.producer.service;

import com.citystream.producer.model.CityEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.city-events:city-events}")
    private String topic;

    private final ObjectMapper objectMapper;

    // Metrics tracking
    private final LongAdder successCount = new LongAdder();
    private final LongAdder failureCount = new LongAdder();
    private final LongAdder totalLatencyMs = new LongAdder();
    private final AtomicLong minLatencyMs = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyMs = new AtomicLong(0);
    private final Instant startTime = Instant.now();

    public KafkaProducerService() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void sendEvent(CityEvent event) {
        Instant sendStart = Instant.now();
        
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            String key = event.getCity();
            
            CompletableFuture<SendResult<String, String>> future = 
                kafkaTemplate.send(topic, key, eventJson);

            future.whenComplete((result, ex) -> {
                long latencyMs = Duration.between(sendStart, Instant.now()).toMillis();
                
                if (ex == null) {
                    // Update success metrics
                    successCount.increment();
                    updateLatencyMetrics(latencyMs);
                    
                    log.info("Sent event: city={}, type={} to partition: {} with offset: {} (latency: {}ms)",
                            event.getCity(),
                            event.getEventType(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset(),
                            latencyMs);
                    
                    // Log metrics every 100 messages
                    if (successCount.sum() % 100 == 0) {
                        logMetrics();
                    }
                } else {
                    // Update failure metrics
                    failureCount.increment();
                    log.error("Failed to send event: city={}, type={} (latency: {}ms)", 
                            event.getCity(), event.getEventType(), latencyMs, ex);
                    logMetrics();
                }
            });
        } catch (JsonProcessingException e) {
            failureCount.increment();
            log.error("Error serializing event: city={}", event.getCity(), e);
        }
    }

    private void updateLatencyMetrics(long latencyMs) {
        totalLatencyMs.add(latencyMs);
        
        // Update min latency
        minLatencyMs.updateAndGet(current -> Math.min(current, latencyMs));
        
        // Update max latency
        maxLatencyMs.updateAndGet(current -> Math.max(current, latencyMs));
    }

    private void logMetrics() {
        long successes = successCount.sum();
        long failures = failureCount.sum();
        long total = successes + failures;
        
        if (total == 0) return;
        
        double successRate = (successes * 100.0) / total;
        double avgLatencyMs = successes > 0 ? (totalLatencyMs.sum() / (double) successes) : 0;
        long uptimeSeconds = Duration.between(startTime, Instant.now()).getSeconds();
        double eventsPerSecond = uptimeSeconds > 0 ? (successes / (double) uptimeSeconds) : 0;
        
        log.info("=== Producer Metrics ===");
        log.info("Total events: {} | Success: {} | Failures: {}", total, successes, failures);
        log.info("Success rate: {:.2f}%", successRate);
        log.info("Latency - Avg: {:.2f}ms | Min: {}ms | Max: {}ms", 
                avgLatencyMs, 
                minLatencyMs.get() == Long.MAX_VALUE ? 0 : minLatencyMs.get(), 
                maxLatencyMs.get());
        log.info("Throughput: {:.2f} events/sec | Uptime: {}s", eventsPerSecond, uptimeSeconds);
        log.info("=======================");
    }

    // Public method to get current metrics (for monitoring endpoints)
    public ProducerMetrics getMetrics() {
        long successes = successCount.sum();
        long failures = failureCount.sum();
        long total = successes + failures;
        
        double successRate = total > 0 ? (successes * 100.0) / total : 0;
        double avgLatencyMs = successes > 0 ? (totalLatencyMs.sum() / (double) successes) : 0;
        long uptimeSeconds = Duration.between(startTime, Instant.now()).getSeconds();
        double eventsPerSecond = uptimeSeconds > 0 ? (successes / (double) uptimeSeconds) : 0;
        
        return new ProducerMetrics(
            total,
            successes,
            failures,
            successRate,
            avgLatencyMs,
            minLatencyMs.get() == Long.MAX_VALUE ? 0 : minLatencyMs.get(),
            maxLatencyMs.get(),
            eventsPerSecond,
            uptimeSeconds
        );
    }

    // Metrics data class
    public static class ProducerMetrics {
        public final long totalEvents;
        public final long successCount;
        public final long failureCount;
        public final double successRate;
        public final double avgLatencyMs;
        public final long minLatencyMs;
        public final long maxLatencyMs;
        public final double eventsPerSecond;
        public final long uptimeSeconds;

        public ProducerMetrics(long totalEvents, long successCount, long failureCount,
                             double successRate, double avgLatencyMs, long minLatencyMs,
                             long maxLatencyMs, double eventsPerSecond, long uptimeSeconds) {
            this.totalEvents = totalEvents;
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.successRate = successRate;
            this.avgLatencyMs = avgLatencyMs;
            this.minLatencyMs = minLatencyMs;
            this.maxLatencyMs = maxLatencyMs;
            this.eventsPerSecond = eventsPerSecond;
            this.uptimeSeconds = uptimeSeconds;
        }
    }
}