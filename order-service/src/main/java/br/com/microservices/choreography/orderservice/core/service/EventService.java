package br.com.microservices.choreography.orderservice.core.service;

import br.com.microservices.choreography.orderservice.config.exception.ValidationException;
import br.com.microservices.choreography.orderservice.core.document.Event;
import br.com.microservices.choreography.orderservice.core.document.History;
import br.com.microservices.choreography.orderservice.core.document.Order;
import br.com.microservices.choreography.orderservice.core.dto.EventFilters;
import br.com.microservices.choreography.orderservice.core.repository.EventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static br.com.microservices.choreography.orderservice.core.enums.ESagaStatus.SUCCESS;
import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@AllArgsConstructor
public class EventService {

    private static final String CURRENT_SERVICE = "ORDER_SERVICE";

    private final EventRepository eventRepository;

    public List<Event> findAll() {
        return eventRepository.findAllByOrderByCreatedAtDesc();
    }

    public Event findByFilters(EventFilters filters) {
        validateEmptyFilters(filters);
        if (!isEmpty(filters.getOrderId())) {
            return findByOrderId(filters.getOrderId());
        } else {
            return findByTransactionId(filters.getTransactionId());
        }
    }

    public void notifyEnding(Event event) {
        event.setSource(CURRENT_SERVICE);
        event.setOrderId(event.getOrderId());
        event.setCreatedAt(LocalDateTime.now());
        setEndingHistory(event);
        save(event);
        log.info("Notify ending event saved {} - {}", event.getOrderId(), event.getTransactionId());
    }

    private void setEndingHistory(Event event) {
        if (SUCCESS.equals(event.getStatus())) {
            log.info("SAGA FINISHED SUCCESSFULLY FOR EVENT: {}", event.getId());
            addHistory(event, "Saga finished successfully!");
        } else {
            log.info("SAGA FINISHED WITH ERROR FOR EVENT: {}", event.getId());
            addHistory(event, "Saga finished with error!");
        }
    }

    private Event findByOrderId(String orderId) {
        return eventRepository
                .findTop1ByOrderIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(() -> new ValidationException("Evento não encontrado"));
    }

    private Event findByTransactionId(String transactionId) {
        return eventRepository
                .findTop1ByTransactionIdOrderByCreatedAtDesc(transactionId)
                .orElseThrow(() -> new ValidationException("Evento não encontrado"));
    }

    private void validateEmptyFilters(EventFilters filters) {
        if (isEmpty(filters.getOrderId()) && isEmpty(filters.getTransactionId())) {
            throw new ValidationException("OrderID or TransactionID must be informed");
        }
    }

    public Event save(Event event) {
        return eventRepository.save(event);
    }

    public Event createEvent(Order order) {
        var event = Event.builder()
                .source(CURRENT_SERVICE)
                .status(SUCCESS)
                .orderId(order.getId())
                .transactionId(order.getTransactionId())
                .payload(order)
                .createdAt(LocalDateTime.now())
                .build();
        addHistory(event, "Saga started");
        save(event);
        return event;
    }

    private void addHistory(Event event, String message) {
        var history = History
                .builder()
                .source(event.getSource())
                .status(event.getStatus())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();

        event.addHistory(history);
    }

}
