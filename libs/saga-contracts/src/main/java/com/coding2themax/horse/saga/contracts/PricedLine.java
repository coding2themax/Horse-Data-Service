package com.coding2themax.horse.saga.contracts;

import java.math.BigDecimal;
import java.util.List;

/** A reserved order line with the unit price set by the stock service. */
public record PricedLine(int productId, int quantity, BigDecimal unitPrice) {

  public PricedLine {
    if (productId <= 0) {
      throw new IllegalArgumentException("productId must be positive");
    }
    if (quantity <= 0) {
      throw new IllegalArgumentException("quantity must be positive");
    }
    if (unitPrice == null || unitPrice.signum() < 0) {
      throw new IllegalArgumentException("unitPrice must not be negative");
    }
  }

  static List<PricedLine> requireLines(List<PricedLine> lines) {
    if (lines == null || lines.isEmpty()) {
      throw new IllegalArgumentException("priced lines must not be empty");
    }
    return List.copyOf(lines);
  }
}
