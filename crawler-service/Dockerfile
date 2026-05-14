# Build stage: compilamos con Maven
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
# Descargamos dependencias primero (mejor caché de Docker)
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -q

# Runtime stage: imagen más ligera para ejecutar
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/crawler-service-1.0.0.jar ./app.jar
COPY --from=build /app/target/libs ./libs

CMD ["java", "-jar", "app.jar"]
