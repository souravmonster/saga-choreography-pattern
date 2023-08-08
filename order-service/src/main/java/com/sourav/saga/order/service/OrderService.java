package com.sourav.saga.order.service;

import com.sourav.saga.common.dto.OrderRequestDto;
import com.sourav.saga.common.event.OrderEvent;
import com.sourav.saga.common.event.OrderStatus;
import com.sourav.saga.common.event.PaymentEvent;
import com.sourav.saga.common.event.PaymentStatus;
import com.sourav.saga.order.entity.PurchaseOrder;
import com.sourav.saga.order.repository.OrderRepository;
import com.sourav.saga.order.util.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@Service
public class OrderService {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private KafkaTemplate<String, OrderEvent> template;

    @Transactional
    public PurchaseOrder createOrder(OrderRequestDto orderRequestDto) {
        PurchaseOrder order = orderRepository.save(ValueMapper.convertDtoToEntity(orderRequestDto));
        orderRequestDto.setOrderId(order.getId());
        //produce kafka event with status ORDER_CREATED
        OrderEvent event = new OrderEvent(orderRequestDto, OrderStatus.ORDER_CREATED);
        template.send("order-event", event);
        return order;
    }

    @KafkaListener(topics = "payment-event", groupId = "payment-event-group")
    public void paymentEventConsumer(PaymentEvent paymentEvent) {
        PurchaseOrder order = orderRepository.findById(paymentEvent.getPaymentRequestDto().getOrderId()).get();
        order.setPaymentStatus(paymentEvent.getPaymentStatus());
        this.updateOrder(order);
    }

    private void updateOrder(PurchaseOrder purchaseOrder) {
        boolean isPaymentComplete = PaymentStatus.PAYMENT_COMPLETED.equals(purchaseOrder.getPaymentStatus());
        OrderStatus orderStatus = isPaymentComplete ? OrderStatus.ORDER_COMPLETED : OrderStatus.ORDER_CANCELLED;
        purchaseOrder.setOrderStatus(orderStatus);
        OrderEvent event = new OrderEvent(ValueMapper.convertEntityToDto(purchaseOrder), orderStatus);
        if (!isPaymentComplete) {
            template.send("order-event", event);
        }
        orderRepository.save(purchaseOrder);
    }

    public List<PurchaseOrder> getAllOrders() {
        return orderRepository.findAll();
    }
}
