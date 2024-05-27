package com.coding2themax.horse.horsedataservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.coding2themax.horse.horsedataservice.handlers.HorseHandler;

@Configuration(proxyBeanMethods = false)
public class HorseRouter {

  @Bean
  public RouterFunction<ServerResponse> route(HorseHandler horseHandler) {
    return RouterFunctions.route()
        .GET("/horses", horseHandler::getAllHorses)
        .build();
  }

}
