package com.coding2themax.horse.order.api;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration(proxyBeanMethods = false)
public class OrderRouter {

  @Bean
  public RouterFunction<ServerResponse> orderRoutes(OrderApiHandler handler) {
    return RouterFunctions.route()
        .POST("/customers", handler::createCustomer)
        .GET("/customers/{id}", handler::getCustomer)
        .POST("/orders", handler::placeOrder)
        .GET("/orders/{id}", handler::getOrder)
        .build();
  }
}
