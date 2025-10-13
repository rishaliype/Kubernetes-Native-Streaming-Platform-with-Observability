package com.citystream.producer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public class CityEvent {
    private String city;
    
    @JsonProperty("event_type")
    private String eventType; // traffic, weather, incident, construction
    
    private String severity; // low, medium, high, critical
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private LocalDateTime timestamp;
    
    private String description;

    public CityEvent() {
        this.timestamp = LocalDateTime.now();
    }

    public CityEvent(String city, String eventType, String severity, String description) {
        this.city = city;
        this.eventType = eventType;
        this.severity = severity;
        this.description = description;
        this.timestamp = LocalDateTime.now();
    }

    // Getters and Setters
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "CityEvent{" +
                "city='" + city + '\'' +
                ", eventType='" + eventType + '\'' +
                ", severity='" + severity + '\'' +
                ", description='" + description + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}