package com.coding2themax.horse.order.application;

import com.coding2themax.horse.order.api.dto.OrderResponse;

/** An order returned by placement; replayed means an earlier request created it. */
public record PlacedOrder(OrderResponse order, boolean replayed) {
}
