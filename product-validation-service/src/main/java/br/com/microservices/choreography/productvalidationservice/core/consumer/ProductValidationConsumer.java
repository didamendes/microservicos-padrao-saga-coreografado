package br.com.microservices.choreography.productvalidationservice.core.consumer;

import br.com.microservices.choreography.productvalidationservice.core.service.ProductValidationService;
import br.com.microservices.choreography.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class ProductValidationConsumer {

    private final JsonUtil jsonUtil;
    private final ProductValidationService service;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.product-validation-start}"
    )
    public void consumeSuccessEvent(String playload) {
        log.info("Consuming notify ending event {} product-validation-start", playload);
        var event = jsonUtil.toEvent(playload);
        service.validateExistingProducts(event);
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.product-validation-fail}"
    )
    public void consumeFailEvent(String playload) {
        log.info("Consuming notify ending event {} product-validation-fail", playload);
        var event = jsonUtil.toEvent(playload);
        service.rollbackEvent(event);
    }

}
