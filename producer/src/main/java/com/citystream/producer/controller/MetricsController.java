package com.citystream.producer.controller;

import com.citystream.producer.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/metrics")
public class MetricsController {

    @Autowired
    private KafkaProducerService producerService;

    @GetMapping("/producer")
    public KafkaProducerService.ProducerMetrics getProducerMetrics() {
        return producerService.getMetrics();
    }
}