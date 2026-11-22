package com.coding2themax.horse.stock;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** JDBC is on the classpath only so Flyway can migrate at startup. */
@AnalyzeClasses(packages = "com.coding2themax.horse.stock", importOptions = ImportOption.DoNotIncludeTests.class)
class NoJdbcInMainCodeArchTest {

  @ArchTest
  static final ArchRule mainCodeMustNotUseBlockingJdbc = noClasses()
      .should().dependOnClassesThat().resideInAnyPackage(
          "java.sql..",
          "javax.sql..",
          "org.springframework.jdbc..",
          "org.springframework.boot.jdbc..",
          "org.springframework.boot.autoconfigure.jdbc..",
          "com.zaxxer.hikari..",
          "org.postgresql..")
      .because("request handling must stay non-blocking on R2DBC");
}
