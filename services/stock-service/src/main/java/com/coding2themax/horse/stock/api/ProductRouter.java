package com.coding2themax.horse.stock.api;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration(proxyBeanMethods = false)
public class ProductRouter {

  @Bean
  public RouterFunction<ServerResponse> productRoutes(ProductHandler handler) {
    return RouterFunctions.route()
        .GET("/products", handler::list)
        .POST("/products", handler::create)
        .GET("/products/{id}", handler::get)
        .PUT("/products/{id}", handler::update)
        .POST("/products/{id}/stock-adjustments", handler::adjustStock)
        .build();
  }
}
