package com.coding2themax.horse.stock.api;

import java.net.URI;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Validator;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.stock.api.dto.ProductDetailsRequest;
import com.coding2themax.horse.stock.api.dto.ProductRequest;
import com.coding2themax.horse.stock.api.dto.ProductResponse;
import com.coding2themax.horse.stock.api.dto.StockAdjustmentRequest;
import com.coding2themax.horse.stock.application.ProductService;
import com.coding2themax.horse.stock.domain.Product;

import reactor.core.publisher.Mono;

@Component
public class ProductHandler {

  private final ProductService products;
  private final Validator validator;

  public ProductHandler(ProductService products, Validator validator) {
    this.products = products;
    this.validator = validator;
  }

  public Mono<ServerResponse> list(ServerRequest request) {
    return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
        .body(products.findAll().map(ProductResponse::from), ProductResponse.class);
  }

  public Mono<ServerResponse> get(ServerRequest request) {
    return Mono.fromCallable(() -> id(request)).flatMap(products::findById).flatMap(ProductHandler::ok);
  }

  public Mono<ServerResponse> create(ServerRequest request) {
    return body(request, ProductRequest.class)
        .flatMap(products::create)
        .map(ProductResponse::from)
        .flatMap(product -> ServerResponse.created(URI.create("/products/" + product.id()))
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(product));
  }

  public Mono<ServerResponse> update(ServerRequest request) {
    return Mono.fromCallable(() -> id(request))
        .flatMap(id -> body(request, ProductDetailsRequest.class).flatMap(body -> products.updateDetails(id, body)))
        .flatMap(ProductHandler::ok);
  }

  public Mono<ServerResponse> adjustStock(ServerRequest request) {
    return Mono.fromCallable(() -> id(request))
        .flatMap(id -> body(request, StockAdjustmentRequest.class)
            .flatMap(body -> products.adjustStock(id, body.delta())))
        .flatMap(ProductHandler::ok);
  }

  private static Mono<ServerResponse> ok(Product product) {
    return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(ProductResponse.from(product));
  }

  private <T> Mono<T> body(ServerRequest request, Class<T> type) {
    return request.bodyToMono(type)
        .switchIfEmpty(Mono.error(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST, "Body is required")))
        .doOnNext(this::validate);
  }

  private void validate(Object body) {
    BeanPropertyBindingResult errors = new BeanPropertyBindingResult(body, "request");
    validator.validate(body, errors);
    if (errors.hasErrors()) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid field: " + errors.getFieldErrors().get(0).getField());
    }
  }

  private static int id(ServerRequest request) {
    try {
      return Integer.parseInt(request.pathVariable("id"));
    } catch (NumberFormatException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Product id must be a number");
    }
  }
}
