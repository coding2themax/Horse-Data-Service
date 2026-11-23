package com.coding2themax.horse.horsedataservice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Verifies that startup adopts a database created by the old init.sql. */
@SpringBootTest
@Testcontainers
class CatalogMigrationUpgradeTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14");

  @DynamicPropertySource
  static void createLegacyDatabase(DynamicPropertyRegistry registry) throws IOException, SQLException {
    String legacySql = new ClassPathResource("db/migration/V1__baseline_schema.sql")
        .getContentAsString(StandardCharsets.UTF_8);
    try (Connection connection = connect(); Statement statement = connection.createStatement()) {
      statement.execute(legacySql);
      statement.execute("""
          INSERT INTO stable.horses (horse_id, horse_status_id, horse_name)
          VALUES (99, 1, 'Existing Horse')
          """);
      statement.execute("""
          INSERT INTO stable.horse_goods (goods_id, goods_name, good_description)
          VALUES (50, 'Existing Good', 'Kept')
          """);
    }
    CatalogDatabaseTestProperties.register(registry, POSTGRES);
  }

  @Test
  void adoptsExistingDatabaseWithoutReapplyingBaseline() throws SQLException {
    try (Connection connection = connect(); Statement statement = connection.createStatement()) {
      assertEquals(4, count(statement, "SELECT count(*) FROM stable.horses"));
      assertEquals(1, count(statement, "SELECT count(*) FROM stable.horses WHERE horse_id = 99"));
      assertEquals(1, count(statement,
          "SELECT count(*) FROM stable.flyway_schema_history WHERE version = '1' AND type = 'BASELINE' AND success"));
      assertEquals(1, count(statement,
          "SELECT count(*) FROM stable.flyway_schema_history WHERE type = 'SQL'"));
      assertEquals(1, count(statement,
          "SELECT count(*) FROM stable.flyway_schema_history WHERE version = '2' AND type = 'SQL' AND success"));
      assertEquals(1, count(statement, "SELECT count(*) FROM stable.horse_goods WHERE goods_id = 50"));
      assertEquals(51, count(statement,
          "INSERT INTO stable.horse_goods (goods_name) VALUES ('Next Good') RETURNING goods_id"));
    }
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static int count(Statement statement, String sql) throws SQLException {
    try (ResultSet result = statement.executeQuery(sql)) {
      result.next();
      return result.getInt(1);
    }
  }
}
