package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void notifyPayment(PaymentProcessedEvent event) {
        log.info("[notification-service] Notificacion de pago: orderId={} customerId={} status={}",
                event.getOrderId(), event.getCustomerId(), event.getStatus());
    }

    @KafkaListener(topics = "inventory", groupId = "notification-service")
    public void notifyInventory(InventoryProcessedEvent event) {
        log.info("[notification-service] Notificacion de inventario: orderId={} customerId={} status={}",
                event.getOrderId(), event.getCustomerId(), event.getStatus());
    }
}
