package com.coding2themax.horse.saga.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcTransactionManagerAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.util.StringUtils;

import io.r2dbc.spi.ConnectionFactory;

/** Adds saga messaging to a service that has R2DBC and Kafka configured. */
@AutoConfiguration(after = { R2dbcAutoConfiguration.class, R2dbcTransactionManagerAutoConfiguration.class,
    KafkaAutoConfiguration.class })
@ConditionalOnClass({ DatabaseClient.class, KafkaTemplate.class })
@ConditionalOnBean({ ConnectionFactory.class, ReactiveTransactionManager.class, KafkaTemplate.class })
@EnableConfigurationProperties(SagaMessagingProperties.class)
public class SagaMessagingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  SagaMessageCodec sagaMessageCodec() {
    return new SagaMessageCodec();
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxStore outboxStore(ConnectionFactory connectionFactory, ReactiveTransactionManager transactionManager,
      SagaMessageCodec codec) {
    return new OutboxStore(DatabaseClient.create(connectionFactory), transactionManager, codec);
  }

  @Bean
  @ConditionalOnMissingBean
  InboxProcessor inboxProcessor(ConnectionFactory connectionFactory, ReactiveTransactionManager transactionManager,
      SagaMessagingProperties properties, @Value("${spring.application.name:}") String applicationName) {
    String consumer = StringUtils.hasText(properties.consumerName()) ? properties.consumerName() : applicationName;
    if (!StringUtils.hasText(consumer)) {
      throw new IllegalStateException(
          "Set horse.saga.messaging.consumer-name or spring.application.name for the saga inbox");
    }
    return new InboxProcessor(DatabaseClient.create(connectionFactory), transactionManager, consumer);
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxPublisher outboxPublisher(KafkaTemplate<String, String> template) {
    return new KafkaOutboxPublisher(template);
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxRelay outboxRelay(ConnectionFactory connectionFactory, ReactiveTransactionManager transactionManager,
      OutboxPublisher publisher, SagaMessagingProperties properties) {
    return new OutboxRelay(DatabaseClient.create(connectionFactory), transactionManager, publisher,
        properties.relay());
  }

  @Bean
  @ConditionalOnMissingBean
  SagaMessageHandler sagaMessageHandler(InboxProcessor inbox, SagaMessageCodec codec, OutboxPublisher publisher,
      SagaMessagingProperties properties) {
    return new SagaMessageHandler(inbox, codec, publisher, properties.retry());
  }
}
