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
    return RouterFunctions.route().GET("/sagas/{id}", handler::get).build();
  }
}
