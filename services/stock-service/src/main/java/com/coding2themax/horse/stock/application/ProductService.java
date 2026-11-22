package com.coding2themax.horse.stock.application;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.stock.api.dto.ProductDetailsRequest;
import com.coding2themax.horse.stock.api.dto.ProductRequest;
import com.coding2themax.horse.stock.domain.Product;
import com.coding2themax.horse.stock.infrastructure.ProductRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ProductService {

  private final ProductRepository products;

  public ProductService(ProductRepository products) {
    this.products = products;
  }

  public Flux<Product> findAll() {
    return products.findAll();
  }

  public Mono<Product> findById(int id) {
    return products.findById(id).switchIfEmpty(notFound(id));
  }

  public Mono<Product> create(ProductRequest request) {
    Product product = new Product();
    product.setName(request.name());
    product.setDescription(request.description());
    product.setPrice(request.price());
    product.setStock(request.stock());
    return products.save(product);
  }

  public Mono<Product> updateDetails(int id, ProductDetailsRequest request) {
    return products.updateDetails(id, request.name(), request.description(), request.price())
        .switchIfEmpty(notFound(id));
  }

  public Mono<Product> adjustStock(int id, int delta) {
    return products.adjustStock(id, delta)
        .switchIfEmpty(Mono.defer(() -> findById(id).flatMap(product -> Mono.error(new ResponseStatusException(
            HttpStatus.CONFLICT, "Stock adjustment is out of range")))));
  }

  private static Mono<Product> notFound(int id) {
    return Mono.error(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Product " + id + " not found"));
  }
}
