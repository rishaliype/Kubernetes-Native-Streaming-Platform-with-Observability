# Stage 1: Build stage
FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /app

# Copy Maven config and source code
COPY pom.xml .
COPY src ./src

# Build the Spring Boot JAR
RUN mvn clean package -DskipTests

# Stage 2: Runtime stage
FROM eclipse-temurin:17-jdk
WORKDIR /app

# Add a non-root user
RUN useradd -m appuser
USER appuser

# Copy the built JAR from the build stage
COPY --from=build /app/target/*.jar app.jar

# Expose the port the microservice will run on
EXPOSE 8081

# Fixed executable
ENTRYPOINT ["java", "-jar", "app.jar"]

# Default runtime arguments
CMD ["--spring.profiles.active=dev"]
