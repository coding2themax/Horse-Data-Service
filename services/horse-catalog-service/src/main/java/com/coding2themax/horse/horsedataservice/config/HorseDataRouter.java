package com.coding2themax.horse.horsedataservice.config;

import org.springdoc.core.annotations.RouterOperation;
import org.springdoc.core.annotations.RouterOperations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.coding2themax.horse.horsedataservice.handlers.HorseGoodHandler;
import com.coding2themax.horse.horsedataservice.handlers.HorseHandler;
import com.coding2themax.horse.horsedataservice.service.HorseService;

@Configuration(proxyBeanMethods = false)
public class HorseDataRouter {

  @RouterOperations({
      @RouterOperation(path = "/horses", method = RequestMethod.GET, beanClass = HorseService.class, beanMethod = "getAllHorses"),
      @RouterOperation(path = "/horses", method = RequestMethod.POST, beanClass = HorseService.class, beanMethod = "createHorse"),
      @RouterOperation(path = "/horses/{id}", method = RequestMethod.GET, beanClass = HorseService.class, beanMethod = "getHorseById"),
      @RouterOperation(path = "/horses/status/{status}", method = RequestMethod.GET, beanClass = HorseService.class, beanMethod = "getHorsesByStatusID"),
      @RouterOperation(path = "/horses/{id}", method = RequestMethod.DELETE, beanClass = HorseService.class, beanMethod = "deleteHorse")
  })
  @Bean
  public RouterFunction<ServerResponse> route(HorseHandler horseHandler) {
    return RouterFunctions.route()
        .GET("/horses", horseHandler::getAllHorses)
        .POST("/horses", horseHandler::createHorse)
        .GET("/horses/{id}", horseHandler::getHorseById)
        .GET("/horses/status/{status}", horseHandler::getHorsesByStatusID)
        .DELETE("horses/{id}", horseHandler::deleteHorse)
        .build();
  }

  @RouterOperations({
      @RouterOperation(path = "/horseGoods", method = RequestMethod.GET, beanClass = HorseGoodHandler.class, beanMethod = "getAllHorses"),
      @RouterOperation(path = "/horseGoods", method = RequestMethod.POST, beanClass = HorseGoodHandler.class, beanMethod = "createHorseGood"),
      @RouterOperation(path = "/horseGoods/{id}", method = RequestMethod.GET, beanClass = HorseGoodHandler.class, beanMethod = "getHorseGoodsById"),
      @RouterOperation(path = "/horseGoods/{id}", method = RequestMethod.DELETE, beanClass = HorseGoodHandler.class, beanMethod = "deleteHorseGood"),
      @RouterOperation(path = "/horseGoods", method = RequestMethod.PUT, beanClass = HorseGoodHandler.class, beanMethod = "updateHorseGood") })
  @Bean
  public RouterFunction<ServerResponse> horseGoodRoute(HorseGoodHandler horseGoodHandler) {
    return RouterFunctions.route()
        .GET("/horseGoods", horseGoodHandler::getAllHorses)
        .POST("/horseGoods", horseGoodHandler::createHorseGood)
        .GET("/horseGoods/{id}", horseGoodHandler::getHorseGoodsById)
        .DELETE("/horseGoods/{id}", horseGoodHandler::deleteHorseGood)
        .PUT("/horseGoods", horseGoodHandler::updateHorseGood)
        .build();
  }

}
