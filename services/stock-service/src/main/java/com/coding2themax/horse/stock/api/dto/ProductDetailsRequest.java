package com.coding2themax.horse.stock.api.dto;

import java.math.BigDecimal;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record ProductDetailsRequest(
    @NotBlank @Size(max = 100) String name,
    String description,
    @NotNull @DecimalMin("0.00") @Digits(integer = 8, fraction = 2) BigDecimal price) {
}
