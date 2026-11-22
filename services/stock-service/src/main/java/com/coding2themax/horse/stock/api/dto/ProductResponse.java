package com.coding2themax.horse.stock.api.dto;

import java.math.BigDecimal;

import com.coding2themax.horse.stock.domain.Product;

public record ProductResponse(Integer id, String name, String description, BigDecimal price, Integer stock) {

  public static ProductResponse from(Product product) {
    return new ProductResponse(product.getId(), product.getName(), product.getDescription(), product.getPrice(),
        product.getStock());
  }
}
