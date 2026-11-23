package com.coding2themax.horse.order.api.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record OrderLineRequest(@NotNull @Positive Integer productId, @NotNull @Positive Integer quantity) {
}
