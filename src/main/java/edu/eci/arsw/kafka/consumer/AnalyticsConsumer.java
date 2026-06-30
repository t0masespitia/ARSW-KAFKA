package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    @KafkaListener(topics = "payments", groupId = "analytics-service")
    public void trackPayment(PaymentProcessedEvent event) {
        log.info("[analytics-service] Pago registrado: orderId={} status={} total={}",
                event.getOrderId(), event.getStatus(), event.getTotal());
    }

    @KafkaListener(topics = "inventory", groupId = "analytics-service")
    public void trackInventory(InventoryProcessedEvent event) {
        log.info("[analytics-service] Inventario registrado: orderId={} status={}",
                event.getOrderId(), event.getStatus());
    }
}
