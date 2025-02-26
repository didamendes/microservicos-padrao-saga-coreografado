package br.com.microservices.choreography.paymentservice.core.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendEvent(String playload, String topic) {
        try {
            log.info("Sending event to kafka {} with payload {}", topic, playload);
            kafkaTemplate.send(topic, playload);
        } catch (Exception e) {
            log.error("Error sending event to kafka", e);
        }
    }


}
