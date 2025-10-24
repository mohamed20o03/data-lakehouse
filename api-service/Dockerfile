# Multi-stage Dockerfile for API service
# Stage 1: build the project
FROM maven:3.9.4-eclipse-temurin-17 AS builder
WORKDIR /workspace

# Copy only what's needed for maven to cache dependencies
COPY service/pom.xml ./
COPY service/src ./src

# Build the jar (skip tests for speed)
RUN mvn -f ./pom.xml -DskipTests package -P!native

# Stage 2: run the application with a slim JDK
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Copy the repackaged Spring Boot jar from the builder
COPY --from=builder /workspace/target/service-0.0.1-SNAPSHOT.jar ./service.jar

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java","-jar","/app/service.jar"]
