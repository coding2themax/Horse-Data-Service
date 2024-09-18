# Use a base image with Java 11 installed
FROM eclipse-temurin:21-jdk-alpine


# Set the working directory inside the container
WORKDIR /app

# Copy the JAR file into the container
COPY target/horse-data-service-0.0.1-SNAPSHOT.jar /app/horse-data-service.jar

# Expose the port that the application will run on
EXPOSE 13000

# Set the command to run the application when the container starts
ENTRYPOINT  ["java", "-jar", "horse-data-service.jar"]