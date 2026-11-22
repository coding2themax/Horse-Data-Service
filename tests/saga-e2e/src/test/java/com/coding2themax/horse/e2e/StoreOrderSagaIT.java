package com.coding2themax.horse.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Runs the real service jars as separate processes against real Postgres and Kafka. */
class StoreOrderSagaIT {

  private static final Duration WAIT = Duration.ofSeconds(90);
  private static final List<String> TOPICS = List.of("horse.order.events", "horse.order.commands",
      "horse.order.replies", "horse.stock.commands", "horse.stock.replies");

  private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14");
  private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");
  private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private static final ObjectMapper JSON = new ObjectMapper();

  private static ServiceProcess stock;
  private static ServiceProcess orders;
  private static ServiceProcess orchestrator;

  @BeforeAll
  static void startSystem() throws Exception {
    POSTGRES.start();
    KAFKA.start();
    try (Connection connection = connect(POSTGRES.getDatabaseName());
        Statement statement = connection.createStatement()) {
      for (String database : List.of("horse_stock", "horse_orders", "horse_saga")) {
        statement.execute("CREATE DATABASE " + database);
      }
    }
    try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      admin.createTopics(TOPICS.stream().map(topic -> new NewTopic(topic, 1, (short) 1)).toList()).all().get();
    }
    stock = new ServiceProcess("stock-service", "horse_stock");
    orders = new ServiceProcess("order-service", "horse_orders");
    orchestrator = new ServiceProcess("saga-orchestrator", "horse_saga");
    stock.start();
    orders.start();
    orchestrator.start();
  }

  @AfterAll
  static void stopSystem() {
    for (ServiceProcess service : new ServiceProcess[] { orchestrator, orders, stock }) {
      if (service != null) {
        service.stop();
      }
    }
    KAFKA.stop();
    POSTGRES.stop();
  }

  @AfterEach
  void restoreServices() throws Exception {
    for (ServiceProcess service : List.of(stock, orders, orchestrator)) {
      if (!service.isRunning()) {
        service.start();
      }
    }
  }

  @Test
  void happyPathApprovesOrderAndTakesStock() throws Exception {
    int product = createProduct(5, "12.50");

    JsonNode placed = placeOrder(product, 2);
    JsonNode saga = awaitSaga(placed, "COMPLETED");

    JsonNode order = getOrder(placed);
    assertEquals("APPROVED", order.get("status").asText());
    assertEquals(25.0, order.get("totalPrice").asDouble());
    assertEquals(12.5, order.get("items").get(0).get("unitPrice").asDouble());
    assertEquals(3, stockOf(product));
    assertEquals(List.of("RESERVING_STOCK", "APPROVING_ORDER", "COMPLETED"), states(saga));
  }

  @Test
  void outOfStockCancelsOrderAndKeepsStock() throws Exception {
    int product = createProduct(1, "10.00");

    JsonNode placed = placeOrder(product, 2);
    JsonNode saga = awaitSaga(placed, "CANCELLED");

    assertEquals("CANCELLED", getOrder(placed).get("status").asText());
    assertEquals(1, stockOf(product));
    assertTrue(saga.get("failureReason").asText().contains("Insufficient stock"));
    assertEquals(List.of("RESERVING_STOCK", "CANCELLING_ORDER", "CANCELLED"), states(saga));
  }

  @Test
  void twoOrdersCompetingForLastUnitNeverOversell() throws Exception {
    int product = createProduct(1, "10.00");
    int customer = createCustomer();

    CompletableFuture<JsonNode> first = CompletableFuture.supplyAsync(() -> placeOrderUnchecked(customer, product));
    CompletableFuture<JsonNode> second = CompletableFuture.supplyAsync(() -> placeOrderUnchecked(customer, product));

    Set<String> finalStates = new HashSet<>();
    finalStates.add(awaitFinished(first.get(30, TimeUnit.SECONDS)).get("state").asText());
    finalStates.add(awaitFinished(second.get(30, TimeUnit.SECONDS)).get("state").asText());

    assertEquals(Set.of("COMPLETED", "CANCELLED"), finalStates);
    assertEquals(0, stockOf(product));
  }

  @Test
  void approvalFailureReleasesReservedStock() throws Exception {
    int product = createProduct(5, "10.00");
    stock.stop();
    JsonNode placed = placeOrder(product, 2);
    awaitSaga(placed, "RESERVING_STOCK");
    // Approval will now fail after stock has been reserved.
    execute("horse_orders", "UPDATE orders SET status = 'CANCELLED' WHERE order_id = '" + text(placed, "id") + "'");

    stock.start();
    JsonNode saga = awaitSaga(placed, "CANCELLED");

    assertEquals(List.of("RESERVING_STOCK", "APPROVING_ORDER", "RELEASING_STOCK", "CANCELLING_ORDER", "CANCELLED"),
        states(saga));
    assertEquals(5, stockOf(product));
  }

  @Test
  void redeliveredMessagesDoNotChangeOutcome() throws Exception {
    int product = createProduct(5, "10.00");
    JsonNode placed = placeOrder(product, 2);
    int stepsBefore = awaitSaga(placed, "COMPLETED").get("steps").size();

    int replayed = replay(text(placed, "sagaId"));
    Thread.sleep(3000);

    assertEquals(5, replayed, "A completed saga has five messages");
    JsonNode saga = getSaga(placed);
    assertEquals("COMPLETED", saga.get("state").asText());
    assertEquals(stepsBefore, saga.get("steps").size());
    assertEquals(3, stockOf(product));
    assertEquals("APPROVED", getOrder(placed).get("status").asText());
  }

  @Test
  void orchestratorRestartMidSagaFinishesSaga() throws Exception {
    int product = createProduct(5, "10.00");
    stock.stop();
    JsonNode placed = placeOrder(product, 1);
    awaitSaga(placed, "RESERVING_STOCK");
    // The saga state commits before the relay publishes its command.
    awaitCommandPublished(placed);

    orchestrator.stop();
    stock.start();
    awaitStock(product, 4);
    orchestrator.start();

    awaitSaga(placed, "COMPLETED");
    assertEquals("APPROVED", getOrder(placed).get("status").asText());
    assertEquals(4, stockOf(product));
  }

  private static int createProduct(int quantity, String price) throws Exception {
    return post(stock, "/products", Map.of("name", "Saddle", "price", price, "stock", quantity), 201).get("id").asInt();
  }

  private static int createCustomer() throws Exception {
    return post(orders, "/customers", Map.of("firstName", "Sam", "lastName", "Rider",
        "email", UUID.randomUUID() + "@example.com"), 201).get("id").asInt();
  }

  private static JsonNode placeOrder(int product, int quantity) throws Exception {
    return post(orders, "/orders", Map.of("customerId", createCustomer(),
        "lines", List.of(Map.of("productId", product, "quantity", quantity))), 202);
  }

  private static JsonNode placeOrderUnchecked(int customer, int product) {
    try {
      return post(orders, "/orders", Map.of("customerId", customer,
          "lines", List.of(Map.of("productId", product, "quantity", 1))), 202);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static JsonNode getOrder(JsonNode placed) throws Exception {
    return get(orders, "/orders/" + text(placed, "id"));
  }

  private static JsonNode getSaga(JsonNode placed) throws Exception {
    return get(orchestrator, "/sagas/" + text(placed, "sagaId"));
  }

  private static int stockOf(int product) throws Exception {
    return get(stock, "/products/" + product).get("stock").asInt();
  }

  private static void awaitStock(int product, int expected) throws Exception {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < deadline) {
      if (stockOf(product) == expected) {
        return;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Stock for product " + product + " never reached " + expected);
  }

  private static void awaitCommandPublished(JsonNode placed) throws Exception {
    String sql = "SELECT count(*) FROM outbox WHERE saga_id = ? AND published_at IS NOT NULL";
    long deadline = System.nanoTime() + WAIT.toNanos();
    try (Connection connection = connect("horse_saga"); PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setObject(1, UUID.fromString(text(placed, "sagaId")));
      while (System.nanoTime() < deadline) {
        try (ResultSet result = statement.executeQuery()) {
          result.next();
          if (result.getInt(1) > 0) {
            return;
          }
        }
        Thread.sleep(50);
      }
    }
    throw new AssertionError("Saga command was never published");
  }

  private static JsonNode awaitSaga(JsonNode placed, String state) throws Exception {
    long deadline = System.nanoTime() + WAIT.toNanos();
    String last = "not found";
    while (System.nanoTime() < deadline) {
      HttpResponse<String> response = send(orchestrator, HttpRequest.newBuilder(orchestrator.uri(
          "/sagas/" + text(placed, "sagaId"))).GET());
      if (response.statusCode() == 200) {
        JsonNode saga = JSON.readTree(response.body());
        last = saga.get("state").asText();
        if (state.equals(last)) {
          return saga;
        }
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Saga stayed in " + last + " instead of reaching " + state);
  }

  private static JsonNode awaitFinished(JsonNode placed) throws Exception {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < deadline) {
      HttpResponse<String> response = send(orchestrator, HttpRequest.newBuilder(orchestrator.uri(
          "/sagas/" + text(placed, "sagaId"))).GET());
      if (response.statusCode() == 200) {
        JsonNode saga = JSON.readTree(response.body());
        String state = saga.get("state").asText();
        if (state.equals("COMPLETED") || state.equals("CANCELLED")) {
          return saga;
        }
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Saga did not finish");
  }

  private static List<String> states(JsonNode saga) {
    List<String> states = new ArrayList<>();
    saga.get("steps").forEach(step -> states.add(step.get("toState").asText()));
    return states;
  }

  private static String text(JsonNode node, String field) {
    return node.get(field).asText();
  }

  /** Sends every stored Kafka record for this saga to its original topic again. */
  private static int replay(String sagaId) {
    Map<String, Object> consumerConfig = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "replay-" + UUID.randomUUID(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    List<ProducerRecord<String, String>> matches = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(),
        new StringDeserializer())) {
      List<TopicPartition> partitions = TOPICS.stream().map(topic -> new TopicPartition(topic, 0)).toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
      while (partitions.stream().anyMatch(partition -> consumer.position(partition) < ends.get(partition))) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
          if (sagaId.equals(record.key())) {
            matches.add(new ProducerRecord<>(record.topic(), record.key(), record.value()));
          }
        }
      }
    }
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new StringSerializer())) {
      matches.forEach(producer::send);
      producer.flush();
    }
    return matches.size();
  }

  private static JsonNode post(ServiceProcess service, String path, Object body, int expected) throws Exception {
    HttpResponse<String> response = send(service, HttpRequest.newBuilder(service.uri(path))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))));
    assertEquals(expected, response.statusCode(), response.body());
    return JSON.readTree(response.body());
  }

  private static JsonNode get(ServiceProcess service, String path) throws Exception {
    HttpResponse<String> response = send(service, HttpRequest.newBuilder(service.uri(path)).GET());
    assertEquals(200, response.statusCode(), response.body());
    return JSON.readTree(response.body());
  }

  private static HttpResponse<String> send(ServiceProcess service, HttpRequest.Builder request) throws Exception {
    return HTTP.send(request.timeout(Duration.ofSeconds(15)).build(), HttpResponse.BodyHandlers.ofString());
  }

  private static Connection connect(String database) throws SQLException {
    String url = "jdbc:postgresql://%s:%d/%s".formatted(POSTGRES.getHost(),
        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), database);
    return DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static void execute(String database, String sql) throws SQLException {
    try (Connection connection = connect(database); PreparedStatement statement = connection.prepareStatement(sql)) {
      assertEquals(1, statement.executeUpdate());
    }
  }

  private static final class ServiceProcess {

    private final String name;
    private final String database;
    private final int port;
    private Process process;

    ServiceProcess(String name, String database) throws IOException {
      this.name = name;
      this.database = database;
      try (ServerSocket socket = new ServerSocket(0)) {
        this.port = socket.getLocalPort();
      }
    }

    URI uri(String path) {
      return URI.create("http://localhost:" + port + path);
    }

    boolean isRunning() {
      return process != null && process.isAlive();
    }

    void start() throws Exception {
      Path services = Path.of(System.getProperty("e2e.services.dir"));
      Path jar = services.resolve(name).resolve("target")
          .resolve(name + "-" + System.getProperty("e2e.version") + ".jar");
      assertTrue(Files.exists(jar), "Missing service jar: " + jar);
      Path logs = Path.of(System.getProperty("e2e.logs.dir"));
      Files.createDirectories(logs);
      File log = logs.resolve(name + ".log").toFile();
      String host = POSTGRES.getHost() + ":" + POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
      ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "-jar", jar.toString(), "--server.port=" + port, "--horse.saga.messaging.relay.poll-interval=100ms",
          "--horse.saga.orchestrator.step-timeout=5m")
          .redirectErrorStream(true)
          .redirectOutput(ProcessBuilder.Redirect.appendTo(log));
      builder.environment().put("DATABASE_HOST", "r2dbc:postgresql://" + host + "/" + database);
      builder.environment().put("DATABASE_JDBC_URL", "jdbc:postgresql://" + host + "/" + database);
      builder.environment().put("DATABASE_USERNAME", POSTGRES.getUsername());
      builder.environment().put("DATABASE_PASSWORD", POSTGRES.getPassword());
      builder.environment().put("KAFKA_BOOTSTRAP_SERVERS", KAFKA.getBootstrapServers());
      process = builder.start();
      awaitHealthy();
    }

    void stop() {
      if (process == null) {
        return;
      }
      process.destroy();
      try {
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
          process.destroyForcibly().waitFor();
        }
      } catch (InterruptedException e) {
        process.destroyForcibly();
        Thread.currentThread().interrupt();
      }
    }

    private void awaitHealthy() throws Exception {
      long deadline = System.nanoTime() + WAIT.toNanos();
      while (System.nanoTime() < deadline) {
        if (!process.isAlive()) {
          throw new AssertionError(name + " exited during startup; see its log in target/e2e-logs");
        }
        try {
          HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(uri("/actuator/health"))
              .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
          if (response.statusCode() == 200) {
            return;
          }
        } catch (IOException e) {
          // Still starting.
        }
        Thread.sleep(200);
      }
      throw new AssertionError(name + " did not become healthy");
    }
  }
}
