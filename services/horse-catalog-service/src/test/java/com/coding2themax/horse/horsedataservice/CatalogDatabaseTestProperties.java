package com.coding2themax.horse.horsedataservice;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;

/** Points the application's real database settings at a test container. */
public final class CatalogDatabaseTestProperties {

  private CatalogDatabaseTestProperties() {
  }

  public static void register(DynamicPropertyRegistry registry, PostgreSQLContainer<?> postgres) {
    registry.add("DATABASE_HOST", () -> "r2dbc:postgresql://%s:%d/%s".formatted(
        postgres.getHost(),
        postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
        postgres.getDatabaseName()));
    registry.add("DATABASE_JDBC_URL", postgres::getJdbcUrl);
    registry.add("DATABASE_USERNAME", postgres::getUsername);
    registry.add("DATABASE_PASSWORD", postgres::getPassword);
  }
}
