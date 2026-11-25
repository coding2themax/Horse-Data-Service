package com.coding2themax.horse.orchestrator.api;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration(proxyBeanMethods = false)
public class SagaRouter {

  @Bean
  public RouterFunction<ServerResponse> sagaRoutes(SagaHandler handler) {
    return RouterFunctions.route()
        // Listed first so "attention" is not read as a saga id.
        .GET("/sagas/attention", handler::needingAttention)
        .GET("/sagas/{id}", handler::get)
        .POST("/sagas/{id}/retry", handler::retry)
        .build();
  }
}
