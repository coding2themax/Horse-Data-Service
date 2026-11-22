package com.coding2themax.horse.stock.infrastructure;

import java.math.BigDecimal;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.coding2themax.horse.stock.domain.Product;

import reactor.core.publisher.Mono;

public interface ProductRepository extends ReactiveCrudRepository<Product, Integer> {

  /** Never writes stock, so a details edit cannot overwrite a concurrent reservation. */
  @Query("""
      UPDATE products SET name = :name, description = :description, price = :price
      WHERE product_id = :id
      RETURNING *
      """)
  Mono<Product> updateDetails(Integer id, String name, String description, BigDecimal price);

  @Query("""
      UPDATE products SET stock = stock + :delta
      WHERE product_id = :id AND stock::bigint + :delta BETWEEN 0 AND 2147483647
      RETURNING *
      """)
  Mono<Product> adjustStock(Integer id, int delta);
}
