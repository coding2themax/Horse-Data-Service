package com.coding2themax.horse.horsedataservice.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.coding2themax.horse.horsedataservice.CatalogDatabaseTestProperties;
import com.coding2themax.horse.horsedataservice.model.Horse;
import com.coding2themax.horse.horsedataservice.service.HorseService;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class HorseRouterTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14");

  @DynamicPropertySource
  static void databaseProperties(DynamicPropertyRegistry registry) {
    CatalogDatabaseTestProperties.register(registry, POSTGRES);
  }

  @Autowired
  private WebTestClient webTestClient;

  @Autowired
  HorseService horseService;

  @Test
  void testRoute() {

    webTestClient
        // Create a GET request to test an endpoint
        .get().uri("/horses")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBodyList(Horse.class).value(h -> {
          Assertions.assertEquals(3, h.size());
        });
  }

  @Test
  void testGetHorseById() {
    webTestClient
        // Create a GET request to test an endpoint
        .get().uri("/horses/1")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBody(Horse.class).value(h -> {
          Assertions.assertEquals(1, h.getId());
        });
  }

  @Test
  void testGetHorseByStatusID() {
    webTestClient
        // Create a GET request to test an endpoint
        .get().uri("/horses/status/1")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBodyList(Horse.class).value(h -> {
          Assertions.assertEquals(1, h.size());
        });
  }

  @Test
  void testCreateHorse() {
    Horse horse = new Horse();
    horse.setName("Max");
    horse.setId(1);
    horse.setAge(5);
    horse.setBreed("Thoroughbred");

    webTestClient
        // Create a POST request to test an endpoint
        .post().uri("/horses")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(horse)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBody(Horse.class).value(h -> {
          Assertions.assertEquals("Max", h.getName());
        });
  }

  @Test
  void testRouteHorseGood() {
    webTestClient
        // Create a GET request to test an endpoint
        .get().uri("/horseGoods")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBodyList(Horse.class).value(h -> {
          Assertions.assertFalse(h.isEmpty());
        });
  }

  @Test
  void testGetHorseGoodById() {
    webTestClient
        // Create a GET request to test an endpoint
        .get().uri("/horseGoods/2")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk();
  }

  @Test
  void testdeleteHorseGoodNotFound() {
    webTestClient
        // Create a DELETE request to test an endpoint
        .delete().uri("/horseGoods/1")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isNotFound().expectBody(Void.class);
  }

  @Test
  void testDeleteHorse() {
    webTestClient
        // Create a DELETE request to test an endpoint
        .delete().uri("/horses/1")
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBody(Horse.class).value(h -> {
          Assertions.assertEquals(1, h.getId());
        });
  }

  @Test
  void createHorseGoodTest() {
    webTestClient
        // Create a POST request to test an endpoint
        .post().uri("/horseGoods")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\n" +
            "  \"id\": 1,\n" +
            "  \"name\": \"Saddle\",\n" +
            "  \"description\": \"description\"\n" +
            "}")
        .exchange()
        // and use the dedicated DSL to test assertions against the response
        .expectStatus().isOk()
        .expectBody(Horse.class).value(h -> {
          Assertions.assertEquals(1, h.getId());
        });
  }

  @Test
  void updateHorseGoodTest() {
    webTestClient.put().uri("/horseGoods")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\n" +
            "  \"id\": 2,\n" +
            "  \"name\": \"Saddle\",\n" +
            "  \"description\": \"description\"\n" +
            "}")
        .exchange()
        .expectStatus().isOk();
  }
}
