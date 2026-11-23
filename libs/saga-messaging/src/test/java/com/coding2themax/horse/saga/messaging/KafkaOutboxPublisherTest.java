package com.coding2themax.horse.saga.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import reactor.core.publisher.Mono;

class KafkaOutboxPublisherTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Test
  void slowSendDoesNotBlockSubscriber() throws InterruptedException {
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);
    AtomicReference<String> sendThread = new AtomicReference<>();
    KafkaOutboxPublisher publisher = new KafkaOutboxPublisher(new StubTemplate(() -> {
      sendThread.set(Thread.currentThread().getName());
      await(release);
      return CompletableFuture.completedFuture(null);
    }));

    publisher.publish("topic", "key", "value").subscribe(null, error -> finished.countDown(), finished::countDown);

    assertEquals(1, finished.getCount(), "subscribe must return while the send is still blocked");
    release.countDown();
    assertTrue(finished.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
    assertNotNull(sendThread.get());
    assertTrue(sendThread.get().startsWith("boundedElastic"), sendThread.get());
  }

  @Test
  void completionDoesNotContinueOnSendThread() {
    CompletableFuture<SendResult<String, String>> result = new CompletableFuture<>();
    KafkaOutboxPublisher publisher = new KafkaOutboxPublisher(new StubTemplate(() -> result));
    AtomicReference<String> continuation = new AtomicReference<>();

    Mono<Void> published = publisher.publish("topic", "key", "value")
        .doOnSuccess(ignored -> continuation.set(Thread.currentThread().getName()));
    CompletableFuture<Void> running = published.toFuture();
    Thread kafkaThread = new Thread(() -> result.complete(null), "kafka-producer-network-thread");
    kafkaThread.start();
    running.orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS).join();

    assertTrue(continuation.get().startsWith("parallel"), continuation.get());
  }

  @Test
  void sendFailureReachesCaller() {
    KafkaOutboxPublisher publisher = new KafkaOutboxPublisher(new StubTemplate(() -> {
      throw new IllegalStateException("broker down");
    }));

    assertThrows(IllegalStateException.class, () -> publisher.publish("topic", "key", "value").block(TIMEOUT));
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @FunctionalInterface
  private interface Send {
    CompletableFuture<SendResult<String, String>> run();
  }

  private static final class StubTemplate extends KafkaTemplate<String, String> {

    private final Send send;

    StubTemplate(Send send) {
      super(new DefaultKafkaProducerFactory<>(Map.of()));
      this.send = send;
    }

    @Override
    public CompletableFuture<SendResult<String, String>> send(String topic, String key, String data) {
      return send.run();
    }
  }
}
