package com.coding2themax.horse.order.api.dto;

import java.math.BigDecimal;

/** The unit price is null until the stock reservation succeeds and the order is approved. */
public record OrderItemResponse(int productId, int quantity, BigDecimal unitPrice) {
}
