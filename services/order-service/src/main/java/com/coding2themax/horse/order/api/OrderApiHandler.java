package com.coding2themax.horse.order.api;

import java.net.URI;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Validator;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.order.api.dto.CreateOrderRequest;
import com.coding2themax.horse.order.api.dto.CustomerRequest;
import com.coding2themax.horse.order.api.dto.CustomerResponse;
import com.coding2themax.horse.order.application.CustomerService;
import com.coding2themax.horse.order.application.OrderService;

import reactor.core.publisher.Mono;

@Component
public class OrderApiHandler {

  private static final String IDEMPOTENCY_KEY_HEADER = "X-Idempotency-Key";
  private static final String IDEMPOTENCY_REPLAYED_HEADER = "X-Idempotency-Replayed";

  private final CustomerService customers;
  private final OrderService orders;
  private final Validator validator;

  public OrderApiHandler(CustomerService customers, OrderService orders, Validator validator) {
    this.customers = customers;
    this.orders = orders;
    this.validator = validator;
  }

  public Mono<ServerResponse> createCustomer(ServerRequest request) {
    return body(request, CustomerRequest.class)
        .flatMap(customers::create)
        .map(CustomerResponse::from)
        .flatMap(customer -> ServerResponse.created(URI.create("/customers/" + customer.id()))
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(customer));
  }

  public Mono<ServerResponse> getCustomer(ServerRequest request) {
    return Mono.fromCallable(() -> customerId(request))
        .flatMap(customers::findById)
        .map(CustomerResponse::from)
        .flatMap(customer -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(customer));
  }

  public Mono<ServerResponse> placeOrder(ServerRequest request) {
    return Mono.fromCallable(() -> idempotencyKey(request))
        .flatMap(key -> body(request, CreateOrderRequest.class).flatMap(order -> orders.place(key, order)))
        .flatMap(placed -> ServerResponse.accepted()
            .location(URI.create("/orders/" + placed.order().id()))
            .header(IDEMPOTENCY_REPLAYED_HEADER, String.valueOf(placed.replayed()))
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(placed.order()));
  }

  public Mono<ServerResponse> getOrder(ServerRequest request) {
    return Mono.fromCallable(() -> orderId(request))
        .flatMap(orders::findById)
        .flatMap(order -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(order));
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
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Invalid field: " + errors.getFieldErrors().get(0).getField());
    }
  }

  private static int customerId(ServerRequest request) {
    try {
      return Integer.parseInt(request.pathVariable("id"));
    } catch (NumberFormatException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Customer id must be a number");
    }
  }

  private static UUID idempotencyKey(ServerRequest request) {
    String value = request.headers().firstHeader(IDEMPOTENCY_KEY_HEADER);
    if (value != null) {
      try {
        UUID key = UUID.fromString(value);
        // UUID.fromString also accepts shortened forms such as 1-1-1-1-1.
        if (key.toString().equalsIgnoreCase(value)) {
          return key;
        }
      } catch (IllegalArgumentException e) {
        // Reported below.
      }
    }
    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, IDEMPOTENCY_KEY_HEADER + " must be a UUID");
  }

  private static UUID orderId(ServerRequest request) {
    try {
      return UUID.fromString(request.pathVariable("id"));
    } catch (IllegalArgumentException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Order id must be a UUID");
    }
  }
}
