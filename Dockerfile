# Build one application from the Maven reactor, selected with --build-arg MODULE=<name>
FROM eclipse-temurin:21-jdk-alpine AS build
ARG MODULE=horse-catalog-service
WORKDIR /workspace
COPY . .
RUN ./mvnw -B -ntp -pl services/${MODULE} -am package -DskipTests \
  && cp services/${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
EXPOSE 13000
ENTRYPOINT ["java", "-jar", "app.jar"]
