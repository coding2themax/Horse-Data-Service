package com.coding2themax.horse.stock.api.dto;

import jakarta.validation.constraints.NotNull;

public record StockAdjustmentRequest(@NotNull Integer delta) {
}
