# Build one application from the Maven reactor:
#   docker build --build-arg MODULE=stock-service --build-arg SERVICE_PORT=13020 .
FROM eclipse-temurin:21-jdk-alpine AS build
ARG MODULE=horse-catalog-service
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
  ./mvnw -B -ntp -pl services/${MODULE} -am package -DskipTests \
  && cp services/${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:21-jre-alpine
ARG SERVICE_PORT=13000
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
EXPOSE ${SERVICE_PORT}
ENTRYPOINT ["java", "-jar", "app.jar"]
