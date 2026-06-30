package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.producer.PaymentEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final PaymentEventProducer paymentProducer;

    public PaymentConsumer(PaymentEventProducer paymentProducer) {
        this.paymentProducer = paymentProducer;
    }

    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void processPayment(OrderCreatedEvent event) {
        log.info("[payment-service] Evento recibido: {}", event);
        boolean approved = event.getTotal() <= 250000;
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                "PAY-" + UUID.randomUUID(),
                event.getOrderId(),
                event.getCustomerId(),
                event.getTotal(),
                approved ? "APPROVED" : "REJECTED",
                Instant.now()
        );
        paymentProducer.publish(paymentEvent);
    }
}
