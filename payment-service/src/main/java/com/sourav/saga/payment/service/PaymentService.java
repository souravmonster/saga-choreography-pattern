package com.sourav.saga.payment.service;

import com.sourav.saga.common.dto.OrderRequestDto;
import com.sourav.saga.common.dto.PaymentRequestDto;
import com.sourav.saga.common.event.OrderEvent;
import com.sourav.saga.common.event.OrderStatus;
import com.sourav.saga.common.event.PaymentEvent;
import com.sourav.saga.common.event.PaymentStatus;
import com.sourav.saga.payment.entity.UserBalance;
import com.sourav.saga.payment.entity.UserTransaction;
import com.sourav.saga.payment.repository.UserBalanceRepository;
import com.sourav.saga.payment.repository.UserTransactionRepository;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class PaymentService {

    @Autowired
    private UserBalanceRepository userBalanceRepository;
    @Autowired
    private UserTransactionRepository userTransactionRepository;

    @Autowired
    private KafkaTemplate<String, PaymentEvent> template;

    @PostConstruct
    public void initUserBalanceInDB() {
        userBalanceRepository.saveAll(Stream.of(new UserBalance(101, 5000),
                new UserBalance(102, 3000),
                new UserBalance(103, 4200),
                new UserBalance(104, 20000),
                new UserBalance(105, 999)).collect(Collectors.toList()));
    }

    /**
     * get the user id
     * check the balance availability
     * if balance sufficient -> Payment completed and deduct amount from DB
     * if payment not sufficient -> cancel order event and update the amount in DB
     **/
    @Transactional
    public void newOrderEvent(OrderEvent orderEvent) {
        OrderRequestDto orderRequestDto = orderEvent.getOrderRequestDto();
        PaymentRequestDto paymentRequestDto = new PaymentRequestDto(orderRequestDto.getOrderId(),
                orderRequestDto.getUserId(), orderRequestDto.getAmount());

        PaymentEvent paymentEvent = userBalanceRepository.findById(orderRequestDto.getUserId())
                .filter(ub -> ub.getPrice() > orderRequestDto.getAmount())
                .map(ub -> {
                    ub.setPrice(ub.getPrice() - orderRequestDto.getAmount());
                    userTransactionRepository.save(new UserTransaction(orderRequestDto.getOrderId(), orderRequestDto.getUserId(), orderRequestDto.getAmount()));
                    return new PaymentEvent(paymentRequestDto, PaymentStatus.PAYMENT_COMPLETED);
                }).orElse(new PaymentEvent(paymentRequestDto, PaymentStatus.PAYMENT_FAILED));
        template.send("payment-event", paymentEvent);
    }

    @Transactional
    public void cancelOrderEvent(OrderEvent orderEvent) {
        userTransactionRepository.findById(orderEvent.getOrderRequestDto().getOrderId())
                .ifPresent(ut -> {
                    userTransactionRepository.delete(ut);
                    userTransactionRepository.findById(ut.getUserId())
                            .ifPresent(ub -> ub.setAmount(ub.getAmount() + ut.getAmount()));
                });
    }


    @KafkaListener(topics = "order-event", groupId = "order-event-group")
    private void processPayment(OrderEvent orderEvent) {
        if (OrderStatus.ORDER_CREATED.equals(orderEvent.getOrderStatus())) {
            this.newOrderEvent(orderEvent);
        } else {
            this.cancelOrderEvent(orderEvent);
        }
    }
}
