package com.coding2themax.horse.order.api.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import com.coding2themax.horse.order.domain.OrderStatus;

public record OrderResponse(UUID id, UUID sagaId, int customerId, OrderStatus status, String statusReason,
    OffsetDateTime orderDate, BigDecimal totalPrice, List<OrderItemResponse> items) {
}
