package com.example.offer.http;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {
    private final ObjectProvider<KafkaListenerEndpointRegistry> registry;

    public HealthController(ObjectProvider<KafkaListenerEndpointRegistry> registry) {
        this.registry = registry;
    }

    @GetMapping("/health/live")
    public Map<String, String> live() {
        return Map.of("status", "UP");
    }

    @GetMapping("/health/ready")
    public ResponseEntity<Map<String, String>> ready() {
        var listeners = registry.getIfAvailable();
        boolean ready = listeners == null || listeners.getListenerContainers().size() == 2
                && listeners.getListenerContainers().stream().allMatch(container -> container.isRunning()
                        && container.getAssignedPartitions() != null && !container.getAssignedPartitions().isEmpty());
        return ResponseEntity.status(ready ? 200 : 503).body(Map.of("status", ready ? "UP" : "OUT_OF_SERVICE"));
    }
}
