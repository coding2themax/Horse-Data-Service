package com.coding2themax.horse.horsedataservice.config;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.coding2themax.horse.horsedataservice.CatalogDatabaseTestProperties;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class HorseGoodsCreateTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14");

  @DynamicPropertySource
  static void databaseProperties(DynamicPropertyRegistry registry) {
    CatalogDatabaseTestProperties.register(registry, POSTGRES);
  }

  @Autowired
  private WebTestClient webTestClient;

  @Test
  void createWithoutIdAssignsIdAndStoresGood() {
    int id = create("{\"name\":\"Halter\",\"description\":\"Nylon\"}");

    webTestClient.get().uri("/horseGoods/{id}", id).exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.name").isEqualTo("Halter")
        .jsonPath("$.description").isEqualTo("Nylon");
  }

  @Test
  void createsGetDistinctIds() {
    assertNotEquals(create("{\"name\":\"Brush\"}"), create("{\"name\":\"Comb\"}"));
  }

  @Test
  void createWithExistingIdDoesNotOverwriteIt() {
    int id = create("{\"id\":3,\"name\":\"Overwrite\",\"description\":\"Wrong\"}");

    assertNotEquals(3, id);
    webTestClient.get().uri("/horseGoods/3").exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.name").isEqualTo("Blanket")
        .jsonPath("$.description").isEqualTo("Wool");
  }

  @Test
  void createWithUnusedIdStillAssignsId() {
    assertNotEquals(500, create("{\"id\":500,\"name\":\"Lead Rope\"}"));
  }

  private int create(String body) {
    Map<String, Object> created = webTestClient.post().uri("/horseGoods")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus().isOk()
        .expectBody(new ParameterizedTypeReference<Map<String, Object>>() {
        })
        .returnResult().getResponseBody();
    assertNotNull(created);
    assertNotNull(created.get("id"));
    return ((Number) created.get("id")).intValue();
  }
}
