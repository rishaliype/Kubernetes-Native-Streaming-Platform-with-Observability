package com.citystream.producer.service;

import com.citystream.producer.model.CityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
public class EventGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(EventGeneratorService.class);

    @Autowired
    private KafkaProducerService producerService;

    private final Random random = new Random();

    private static final String[] EVENT_TYPES = {"traffic", "weather", "incident", "construction"};
    private static final String[] CITIES = {"SF", "NYC", "LA", "Chicago", "Seattle", "Boston"};
    private static final String[] SEVERITIES = {"low", "medium", "high", "critical"};

    // Generate event every 5 seconds
    @Scheduled(fixedDelay = 5000, initialDelay = 3000)
    public void generateEvent() {
        String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
        String city = CITIES[random.nextInt(CITIES.length)];
        String severity = SEVERITIES[random.nextInt(SEVERITIES.length)];
        
        CityEvent event = new CityEvent(
                city,
                eventType,
                severity,
                generateDescription(eventType, severity)
        );

        log.info("Generated event: {}", event);
        producerService.sendEvent(event);
    }

    private String generateDescription(String eventType, String severity) {
        return switch (eventType) {
            case "traffic" -> severity + " traffic congestion detected";
            case "weather" -> severity + " weather condition reported";
            case "incident" -> severity + " incident reported, emergency services notified";
            case "construction" -> severity + " construction work in progress";
            default -> "City event detected";
        };
    }
}