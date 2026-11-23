package com.coding2themax.horse.saga.contracts;

import java.util.List;

/** A product and quantity requested by an order. */
public record OrderLine(int productId, int quantity) {

  public OrderLine {
    if (productId <= 0) {
      throw new IllegalArgumentException("productId must be positive");
    }
    if (quantity <= 0) {
      throw new IllegalArgumentException("quantity must be positive");
    }
  }

  static List<OrderLine> requireLines(List<OrderLine> lines) {
    if (lines == null || lines.isEmpty()) {
      throw new IllegalArgumentException("an order must contain at least one line");
    }
    return List.copyOf(lines);
  }
}
