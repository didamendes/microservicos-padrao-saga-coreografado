package br.com.microservices.choreography.orderservice.core.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic.product-validation-start}")
    private String productValidationStartTopic;

    public void sendEvent(String playload) {
        try {
            log.info("Sending event to kafka {} with payload {}", productValidationStartTopic, playload);
            kafkaTemplate.send(productValidationStartTopic, playload);
        } catch (Exception e) {
            log.error("Error sending event to kafka", e);
        }
    }


}
