package com.coding2themax.horse.orchestrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.coding2themax.horse.orchestrator.application.OrchestratorProperties;

@SpringBootApplication
@EnableConfigurationProperties(OrchestratorProperties.class)
public class SagaOrchestratorApplication {

  public static void main(String[] args) {
    SpringApplication.run(SagaOrchestratorApplication.class, args);
  }
}
